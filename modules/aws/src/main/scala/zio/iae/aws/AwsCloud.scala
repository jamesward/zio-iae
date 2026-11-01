package zio.iae.aws

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*

/** How the runtime layer treats resources that don't match the declaration. */
enum ProvisionMode derives CanEqual:
  /** Production: bind only to resources the engine provisioned from the same spec. */
  case BindOnly

  /** Development and tests: create or converge resources on demand, without the engine. */
  case AutoProvision

final case class AwsCloud(app: AppName, env: EnvName, mode: ProvisionMode, s3: S3Client, buckets: BucketProvider)
    extends Cloud:
  def bucket[I](id: LogicalId, spec: BucketSpec): IO[InfraError, Bucket[I]] =
    val address = Address(app, env, ResourceKind.Bucket, id)
    for
      name     <- ZIO.fromEither(buckets.naming.physical(address))
      observed <- buckets.observe(address).mapError(InfraError.Provider(_))
      _ <- mode match
             case ProvisionMode.AutoProvision =>
               ZIO.foreachDiscard(buckets.diff(address, spec, observed))(c => buckets.applyChange(c, ApplyContext.local).mapError(InfraError.Provider(_)))
             case ProvisionMode.BindOnly => verify(address, spec, observed)
    yield S3Bucket[I](s3, name)

  private def verify(address: Address, spec: BucketSpec, observed: Option[ObservedBucket]): IO[InfraError, Unit] =
    val expected = SpecHash.of(ResourceSpec.Bucket(spec))
    observed match
      case None => ZIO.fail(InfraError.NotProvisioned(address))
      case Some(bucket) =>
        val actual = ManagedTags.specHash(bucket.tags)
        ZIO.fail(InfraError.SpecMismatch(address, expected, actual)).unless(actual.contains(expected)).unit

object AwsCloud:
  def layer(app: AppName, env: EnvName, mode: ProvisionMode, naming: BucketNaming): ZLayer[S3Client, Nothing, Cloud] =
    ZLayer.fromFunction((s3: S3Client) => AwsCloud(app, env, mode, s3, BucketProvider(s3, naming)))

  /** Configuration from the environment: `IAE_ENV` (required), `IAE_PROVISION` (`bind`, the default, or `auto`),
    * `IAE_NAME_SUFFIX`, and the standard AWS variables.
    */
  def fromEnvironment(app: AppName): ZLayer[Any, InfraError, Cloud] =
    val settings =
      for
        envName <- System.env("IAE_ENV").orElseFail(InfraError.InvalidConfig("cannot read IAE_ENV")).someOrFail(InfraError.InvalidConfig("IAE_ENV is not set"))
        env     <- ZIO.fromEither(EnvName.from(envName)).mapError(e => InfraError.InvalidConfig(e.message))
        modeS   <- System.env("IAE_PROVISION").orElseFail(InfraError.InvalidConfig("cannot read IAE_PROVISION"))
        mode <- modeS match
                  case None | Some("bind") => ZIO.succeed(ProvisionMode.BindOnly)
                  case Some("auto")        => ZIO.succeed(ProvisionMode.AutoProvision)
                  case Some(other)         => ZIO.fail(InfraError.InvalidConfig(s"IAE_PROVISION must be 'bind' or 'auto', not '$other'"))
        suffix <- System.env("IAE_NAME_SUFFIX").orElseFail(InfraError.InvalidConfig("cannot read IAE_NAME_SUFFIX"))
      yield (env, mode, BucketNaming(suffix))
    val clients =
      AwsConfig.layer.mapError(e => InfraError.InvalidConfig(e.message)) ++
        Client.default.mapError(e => InfraError.Unavailable(s"HTTP client: ${e.getMessage}"))
    ZLayer.fromZIO(settings).flatMap: env =>
      val (envName, mode, naming) = env.get
      (clients >>> S3Client.layer) >>> layer(app, envName, mode, naming)
