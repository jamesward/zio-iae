package zio.iae.engine

import zio.*
import zio.iae.aws.*
import zio.iae.aws.client.{IamClient, S3Client, SecretsClient, SqsClient, TaggingClient}

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API plus IAM. */
object AwsEngine:
  def drivers(s3: S3Client, sqs: SqsClient, secrets: SecretsClient, iam: IamClient, naming: BucketNaming, context: AwsContext): Drivers =
    Drivers(
      List(
        BucketDriver(BucketProvider(s3, naming)),
        QueueDriver(QueueProvider(sqs)),
        SecretDriver(SecretProvider(secrets)),
        RoleDriver(RoleProvider(iam, naming, context)),
      )
    )

  def discovery(tagging: TaggingClient, iam: IamClient): CompositeDiscovery =
    CompositeDiscovery(List(TaggingDiscovery(tagging), RoleDiscovery(iam)))

  def layer(naming: BucketNaming): ZLayer[S3Client & SqsClient & SecretsClient & TaggingClient & IamClient & AwsContext & CommitOrder, Nothing, Engine] =
    ZLayer.fromFunction:
      (s3: S3Client, sqs: SqsClient, secrets: SecretsClient, tagging: TaggingClient, iam: IamClient, context: AwsContext, commits: CommitOrder) =>
        Engine.make(drivers(s3, sqs, secrets, iam, naming, context), discovery(tagging, iam), commits)
