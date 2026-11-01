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

  def risk(change: Change[BucketSpec]): Risk = BucketProvider.risk(change)

  def applyChange(change: Change[BucketSpec]): IO[ProviderError, Unit] =
    val address = change.address
    val effect = nameOf(address).flatMap: name =>
      change match
        case Change.Create(_, spec) =>
          s3.createBucket(name).catchSome { case S3Error.BucketAlreadyOwnedByYou(_) => ZIO.unit }.mapError(failed(address, "create")) *>
            converge(address, name, spec)
        case Change.Update(_, spec, _) => converge(address, name, spec)
        case Change.Delete(_) =>
          s3.deleteBucket(name).catchSome { case S3Error.NoSuchBucket(_) => ZIO.unit }.mapError(failed(address, "delete"))
    effect.tapError(e => ZIO.logWarning(e.message))

  /** Brings an existing bucket to the spec. Reads before writing so it can be re-run safely. */
  private def converge(address: Address, name: BucketName, spec: BucketSpec): IO[ProviderError, Unit] =
    for
      versioning <- s3.getBucketVersioning(name).mapError(failed(address, "read versioning"))
      _ <- s3.putBucketVersioning(name, enabled = spec.versioning == Versioning.Enabled)
             .mapError(failed(address, "set versioning"))
             .when(BucketProvider.versioningChanges(spec.versioning, versioning))
      tags <- s3.getBucketTagging(name).mapError(failed(address, "read tags"))
      merged = tags.filterNot((k, _) => ManagedTags.isManaged(k)) ++ BucketProvider.desiredTags(address, spec)
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
        val tags = desiredTags(address, desired).toList.sorted.collect:
          case (key, value) if !current.tags.get(key).contains(value) =>
            FieldChange(s"tag $key", current.tags.get(key).fold("(none)")(identity), value)
        NonEmptyChunk.fromIterableOption(versioning.toList ++ tags).map(Change.Update(address, desired, _))

  def risk(change: Change[BucketSpec]): Risk = change match
    case Change.Create(_, _)    => Risk.Low
    case Change.Update(_, _, _) => Risk.Medium
    case Change.Delete(_)       => Risk.Critical

  private[aws] def desiredTags(address: Address, spec: BucketSpec): Map[String, String] =
    ManagedTags.of(address, SpecHash.of(ResourceSpec.Bucket(spec)))

  private[aws] def versioningChanges(desired: Versioning, current: VersioningStatus): Boolean = (desired, current) match
    case (Versioning.Enabled, VersioningStatus.Enabled)                           => false
    case (Versioning.Enabled, VersioningStatus.Off | VersioningStatus.Suspended)  => true
    case (Versioning.Disabled, VersioningStatus.Enabled)                          => true
    case (Versioning.Disabled, VersioningStatus.Off | VersioningStatus.Suspended) => false
