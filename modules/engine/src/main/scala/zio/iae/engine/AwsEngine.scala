package zio.iae.engine

import zio.*
import zio.iae.aws.*
import zio.iae.aws.client.{S3Client, TaggingClient}

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API. */
object AwsEngine:
  def drivers(s3: S3Client, naming: BucketNaming): Drivers =
    Drivers(List(BucketDriver(BucketProvider(s3, naming))))

  def layer(naming: BucketNaming): ZLayer[S3Client & TaggingClient & CommitOrder, Nothing, Engine] =
    ZLayer.fromFunction: (s3: S3Client, tagging: TaggingClient, commits: CommitOrder) =>
      Engine.make(drivers(s3, naming), TaggingDiscovery(tagging), commits)
