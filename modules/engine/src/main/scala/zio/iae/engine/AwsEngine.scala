package zio.iae.engine

import zio.*
import zio.iae.aws.*
import zio.iae.aws.client.{IamClient, S3Client, TaggingClient}

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API plus IAM. */
object AwsEngine:
  def drivers(s3: S3Client, iam: IamClient, naming: BucketNaming): Drivers =
    Drivers(List(BucketDriver(BucketProvider(s3, naming)), RoleDriver(RoleProvider(iam, naming))))

  def discovery(tagging: TaggingClient, iam: IamClient): CompositeDiscovery =
    CompositeDiscovery(List(TaggingDiscovery(tagging), RoleDiscovery(iam)))

  def layer(naming: BucketNaming): ZLayer[S3Client & TaggingClient & IamClient & CommitOrder, Nothing, Engine] =
    ZLayer.fromFunction: (s3: S3Client, tagging: TaggingClient, iam: IamClient, commits: CommitOrder) =>
      Engine.make(drivers(s3, iam, naming), discovery(tagging, iam), commits)
