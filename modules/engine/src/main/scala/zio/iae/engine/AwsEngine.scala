package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.aws.*
import zio.iae.aws.client.*

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API plus IAM. */
object AwsEngine:
  def drivers(s3: S3Client, sqs: SqsClient, secrets: SecretsClient, dynamo: DynamoClient, sns: SnsClient, iam: IamClient, naming: BucketNaming, context: AwsContext): Drivers =
    Drivers(
      List(
        BucketDriver(BucketProvider(s3, naming)),
        QueueDriver(QueueProvider(sqs)),
        SecretDriver(SecretProvider(secrets)),
        TableDriver(TableProvider(dynamo)),
        TopicDriver(TopicProvider(sns, sqs, context)),
        RoleDriver(RoleProvider(iam, naming, context)),
      )
    )

  def discovery(tagging: TaggingClient, iam: IamClient): CompositeDiscovery =
    CompositeDiscovery(List(TaggingDiscovery(tagging), RoleDiscovery(iam)))

  type Clients = AwsCloud.Clients & TaggingClient & IamClient

  /** Every client the engine needs, from an `AwsConfig` and an HTTP client. */
  val clients: ZLayer[AwsConfig & Client, AwsError, Clients] =
    AwsCloud.clients ++ ((AwsJsonClient.layer >>> TaggingClient.layer) ++ (AwsQueryClient.layer >>> IamClient.layer))

  /** The engine from one set of clients for both planning and applying (a single account, the CLI, tests). */
  def layer(naming: BucketNaming): ZLayer[Clients & CommitOrder, Nothing, Engine] =
    ZLayer:
      for
        clients <- ZIO.environment[Clients]
        commits <- ZIO.service[CommitOrder]
      yield make(naming, clients, clients, commits)

  /** Plans with the planner's clients and applies with the deployer's, e.g. from an env account's read-only role and
    * its deploy role.
    */
  def make(naming: BucketNaming, planner: ZEnvironment[Clients], deployer: ZEnvironment[Clients], commits: CommitOrder): Engine =
    def driversOf(c: ZEnvironment[Clients]) =
      drivers(c.get[S3Client], c.get[SqsClient], c.get[SecretsClient], c.get[DynamoClient], c.get[SnsClient], c.get[IamClient], naming, c.get[AwsContext])
    Engine(Planner(driversOf(planner), discovery(planner.get[TaggingClient], planner.get[IamClient])), zio.iae.engine.Executor(driversOf(deployer), commits))

  /** The clients for one account's config. */
  def clientsFor(config: AwsConfig): ZIO[Client & Scope, AwsError, ZEnvironment[Clients]] =
    (ZLayer.succeed(config) ++ ZLayer.environment[Client] >>> clients).build
