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

final case class AwsCloud(
    app: AppName,
    env: EnvName,
    mode: ProvisionMode,
    s3: S3Client,
    buckets: BucketProvider,
    sqs: SqsClient,
    queues: QueueProvider,
    secrets: SecretsClient,
    dynamo: DynamoClient,
    sns: SnsClient,
    context: AwsContext,
) extends Cloud:
  def bucket[I](id: LogicalId, spec: BucketSpec): IO[InfraError, Bucket[I]] =
    val address = Address(app, env, ResourceKind.Bucket, id)
    for
      name <- ZIO.fromEither(buckets.naming.physical(address))
      _    <- bind(buckets, address, spec, ResourceSpec.Bucket(spec))(_.tags)
    yield S3Bucket[I](s3, name)

  def queue[I <: MessageQueue.Id[?]](id: I): IO[InfraError, MessageQueue[I]] =
    val address = Address(app, env, ResourceKind.Queue, id.logicalId)
    for
      observed <- bind(queues, address, id.spec, ResourceSpec.Queue(id.spec))(_.tags)
      url      <- ZIO.fromOption(observed.map(_.url)).orElseFail(InfraError.NotProvisioned(address))
    yield SqsQueue[I](id, sqs, url)

  def secret[I](id: LogicalId, spec: SecretSpec): IO[InfraError, SecretReader[I]] =
    val address = Address(app, env, ResourceKind.Secret, id)
    bind(SecretProvider(secrets), address, spec, ResourceSpec.Secret(spec))(_.tags).as(AwsSecret[I](id, secrets, SecretNaming.physical(address)))

  def table[I <: Table.Id[?, ?]](id: I): IO[InfraError, Table[I]] =
    val address = Address(app, env, ResourceKind.Table, id.logicalId)
    bind(TableProvider(dynamo), address, id.spec, ResourceSpec.Table(id.spec))(_.tags).as(DynamoKeyValue[I](id, dynamo, TableNaming.physical(address)))

  def topic[I <: Topic.Id[?]](id: I): IO[InfraError, TopicPublisher[I]] =
    val address = Address(app, env, ResourceKind.Topic, id.logicalId)
    bind(TopicProvider(sns, sqs, context), address, id.spec, ResourceSpec.Topic(id.spec))(_.tags)
      .as(SnsTopic[I](id, sns, TopicNaming.arn(context, address)))

  /** Checks (or, when auto-provisioning, converges) the resource, and returns what is there afterwards. */
  private def bind[S, O](provider: Provider[S, O], address: Address, spec: S, resource: ResourceSpec)(tags: O => Map[String, String]): IO[InfraError, Option[O]] =
    for
      observed <- provider.observe(address).mapError(InfraError.Provider(_))
      after <- mode match
                 case ProvisionMode.AutoProvision =>
                   provider.diff(address, spec, observed) match
                     case None => ZIO.succeed(observed)
                     case Some(change) =>
                       provider.applyChange(change, ApplyContext.local).mapError(InfraError.Provider(_)) *>
                         provider.observe(address).mapError(InfraError.Provider(_))
                 case ProvisionMode.BindOnly =>
                   val expected = SpecHash.of(resource)
                   observed match
                     case None => ZIO.fail(InfraError.NotProvisioned(address))
                     case Some(o) =>
                       val actual = ManagedTags.specHash(tags(o))
                       ZIO.fail(InfraError.SpecMismatch(address, expected, actual)).unless(actual.contains(expected)).as(observed)
    yield after

object AwsCloud:
  type Clients = S3Client & SqsClient & SecretsClient & DynamoClient & SnsClient & AwsContext

  def layer(app: AppName, env: EnvName, mode: ProvisionMode, naming: BucketNaming): ZLayer[Clients, Nothing, Cloud] =
    ZLayer:
      for
        s3      <- ZIO.service[S3Client]
        sqs     <- ZIO.service[SqsClient]
        secrets <- ZIO.service[SecretsClient]
        dynamo  <- ZIO.service[DynamoClient]
        sns     <- ZIO.service[SnsClient]
        context <- ZIO.service[AwsContext]
      yield AwsCloud(app, env, mode, s3, BucketProvider(s3, naming), sqs, QueueProvider(sqs), secrets, dynamo, sns, context)

  /** Every client the AWS cloud needs, from an `AwsConfig` and an HTTP client. */
  val clients: ZLayer[AwsConfig & Client, AwsError, Clients] =
    ZLayer.makeSome[AwsConfig & Client, Clients](
      S3Client.layer,
      AwsJsonClient.layer,
      SqsClient.layer,
      SecretsClient.layer,
      DynamoClient.layer,
      AwsQueryClient.layer,
      SnsClient.layer,
      StsClient.layer,
      AwsContext.layer,
    )

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
    val base =
      AwsConfig.layer.mapError(e => InfraError.InvalidConfig(e.message)) ++
        Client.default.mapError(e => InfraError.Unavailable(s"HTTP client: ${e.getMessage}"))
    ZLayer.fromZIO(settings).flatMap: env =>
      val (envName, mode, naming) = env.get
      (base >>> clients.mapError(e => InfraError.Unavailable(e.message))) >>> layer(app, envName, mode, naming)
