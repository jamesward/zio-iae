package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

final case class ObservedBucket(name: BucketName, versioning: VersioningStatus, tags: Map[String, String])

final case class BucketProvider(s3: S3Client, naming: BucketNaming) extends Provider[BucketSpec, ObservedBucket]:
  val kind: ResourceKind = ResourceKind.Bucket

  def observe(address: Address): IO[ProviderError, Option[ObservedBucket]] =
    for
      name   <- nameOf(address)
      exists <- s3.bucketExists(name).mapError(failed(address, "observe"))
      observed <- ZIO.when(exists):
                    s3.getBucketVersioning(name)
                      .zipPar(s3.getBucketTagging(name))
                      .mapBoth(failed(address, "observe"), (v, t) => ObservedBucket(name, v, t))
    yield observed

  def diff(address: Address, desired: BucketSpec, observed: Option[ObservedBucket]): Option[Change[BucketSpec]] =
    BucketProvider.diff(address, desired, observed)

  def assess(change: Change[BucketSpec], observed: Option[ObservedBucket]): Assessment = BucketProvider.assess(change)

  def applyChange(change: Change[BucketSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    val effect = nameOf(address).flatMap: name =>
      change match
        case Change.Create(_, spec) =>
          s3.createBucket(name).catchSome { case S3Error.BucketAlreadyOwnedByYou(_) => ZIO.unit }.mapError(failed(address, "create")) *>
            converge(address, name, spec, context)
        case Change.Update(_, spec, _) => converge(address, name, spec, context)
        case Change.Orphan(_, since) =>
          for
            tags <- s3.getBucketTagging(name).mapError(failed(address, "read tags"))
            _    <- s3.putBucketTagging(name, ManagedTags.orphaned(tags, since, context)).mapError(failed(address, "tag as orphaned"))
          yield ()
        case Change.Delete(_) =>
          s3.deleteBucket(name).catchSome { case S3Error.NoSuchBucket(_) => ZIO.unit }.mapError(failed(address, "delete"))
        case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))
    effect.tapError(e => ZIO.logWarning(e.message))

  /** Brings an existing bucket to the spec. Reads before writing so it can be re-run safely. */
  private def converge(address: Address, name: BucketName, spec: BucketSpec, context: ApplyContext): IO[ProviderError, Unit] =
    for
      versioning <- s3.getBucketVersioning(name).mapError(failed(address, "read versioning"))
      _ <- s3.putBucketVersioning(name, enabled = spec.versioning == Versioning.Enabled)
             .mapError(failed(address, "set versioning"))
             .when(BucketProvider.versioningChanges(spec.versioning, versioning))
      tags <- s3.getBucketTagging(name).mapError(failed(address, "read tags"))
      merged = ManagedTags.converged(tags, address, SpecHash.of(ResourceSpec.Bucket(spec)), context)
      _ <- s3.putBucketTagging(name, merged).mapError(failed(address, "tag")).when(merged != tags)
    yield ()

  private def nameOf(address: Address): IO[ProviderError, BucketName] =
    ZIO.fromEither(naming.physical(address)).mapError(e => ProviderError(address, "name", e.message))

  private def failed(address: Address, operation: String)(error: S3Error): ProviderError =
    ProviderError(address, operation, error.message)

object BucketProvider:
  /** Pure: what has to change to bring the observed bucket to the desired spec. */
  def diff(address: Address, desired: BucketSpec, observed: Option[ObservedBucket]): Option[Change[BucketSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val versioning = Option.when(versioningChanges(desired.versioning, current.versioning)):
          FieldChange("versioning", current.versioning.toString, desired.versioning.toString)
        val tags = ManagedTags.differences(current.tags, desiredTags(address, desired))
        NonEmptyChunk.fromIterableOption(versioning.toList ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[BucketSpec]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Medium, Set.empty)
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.Critical, Set(ChangeFlag.DataLoss))
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  private[aws] def desiredTags(address: Address, spec: BucketSpec): Map[String, String] =
    ManagedTags.identity(address, SpecHash.of(ResourceSpec.Bucket(spec)))

  private[aws] def versioningChanges(desired: Versioning, current: VersioningStatus): Boolean = (desired, current) match
    case (Versioning.Enabled, VersioningStatus.Enabled)                           => false
    case (Versioning.Enabled, VersioningStatus.Off | VersioningStatus.Suspended)  => true
    case (Versioning.Disabled, VersioningStatus.Enabled)                          => true
    case (Versioning.Disabled, VersioningStatus.Off | VersioningStatus.Suspended) => false

/** The bucket kind, as the engine sees it. */
final case class BucketDriver(provider: BucketProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Bucket

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Bucket(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Bucket(_)), assessment.risk, assessment.flags ++ Drifted.flags(change))
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a bucket spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[BucketSpec]] = change match
      case Change.Create(address, ResourceSpec.Bucket(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Bucket(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                             => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                    => Right(Change.Delete(address))
      case other                                                     => Left(ProviderError(other.address, "apply", "expected a bucket change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))
