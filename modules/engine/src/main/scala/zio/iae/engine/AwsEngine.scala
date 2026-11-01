package zio.iae.engine

import zio.*
import zio.iae.aws.*
import zio.iae.aws.client.{DynamoClient, IamClient, S3Client, SecretsClient, SqsClient, TaggingClient}

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API plus IAM. */
object AwsEngine:
  def drivers(s3: S3Client, sqs: SqsClient, secrets: SecretsClient, dynamo: DynamoClient, iam: IamClient, naming: BucketNaming, context: AwsContext): Drivers =
    Drivers(
      List(
        BucketDriver(BucketProvider(s3, naming)),
        QueueDriver(QueueProvider(sqs)),
        SecretDriver(SecretProvider(secrets)),
        TableDriver(TableProvider(dynamo)),
        RoleDriver(RoleProvider(iam, naming, context)),
      )
    )

  def discovery(tagging: TaggingClient, iam: IamClient): CompositeDiscovery =
    CompositeDiscovery(List(TaggingDiscovery(tagging), RoleDiscovery(iam)))

  type Clients = S3Client & SqsClient & SecretsClient & DynamoClient & TaggingClient & IamClient

  def layer(naming: BucketNaming): ZLayer[Clients & AwsContext & CommitOrder, Nothing, Engine] =
    ZLayer:
      for
        s3      <- ZIO.service[S3Client]
        sqs     <- ZIO.service[SqsClient]
        secrets <- ZIO.service[SecretsClient]
        dynamo  <- ZIO.service[DynamoClient]
        tagging <- ZIO.service[TaggingClient]
        iam     <- ZIO.service[IamClient]
        context <- ZIO.service[AwsContext]
        commits <- ZIO.service[CommitOrder]
      yield Engine.make(drivers(s3, sqs, secrets, dynamo, iam, naming, context), discovery(tagging, iam), commits)
