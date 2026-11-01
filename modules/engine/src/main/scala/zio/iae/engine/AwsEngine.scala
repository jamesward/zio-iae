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

  def layer(naming: BucketNaming): ZLayer[Clients & CommitOrder, Nothing, Engine] =
    ZLayer:
      for
        s3      <- ZIO.service[S3Client]
        sqs     <- ZIO.service[SqsClient]
        secrets <- ZIO.service[SecretsClient]
        dynamo  <- ZIO.service[DynamoClient]
        sns     <- ZIO.service[SnsClient]
        tagging <- ZIO.service[TaggingClient]
        iam     <- ZIO.service[IamClient]
        context <- ZIO.service[AwsContext]
        commits <- ZIO.service[CommitOrder]
      yield Engine.make(drivers(s3, sqs, secrets, dynamo, sns, iam, naming, context), discovery(tagging, iam), commits)
