package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Storing an app's data for less: a bucket holding at least `minGb` in S3 Standard without iae's Intelligent-Tiering
  * rule, which would move what nobody reads for 30 days to the infrequent tier. The saving assumes half the data goes
  * there (S3 can't say which objects are read without its paid analytics), less tiering's monitoring fee per object.
  * DynamoDB tables only have on-demand capacity in `TableSpec`, so there's no capacity mode to switch to yet.
  */
final case class StorageAdvice(s3: S3Client, cloudwatch: CloudWatchClient, discovery: Discovery, naming: BucketNaming, prices: Prices, minGb: Double = 50)
    extends Recommendations:
  def services(app: AppName, env: EnvName): IO[ProviderError, List[RightSizing]] = ZIO.succeed(Nil)

  override def storage(app: AppName, env: EnvName): IO[ProviderError, List[StorageFinding]] =
    for
      found <- discovery.managed(app, env)
      book  <- prices.book
      now   <- Clock.instant
      since  = now.minusSeconds(3L * 24 * 3600).nn
      findings <- ZIO.foreach(found.filter(d => d.address.kind == ResourceKind.Bucket && d.orphanedSince.isEmpty)): d =>
                    ZIO.foreach(naming.physical(d.address).toOption): bucket =>
                      def latest(metric: String, storage: String) =
                        cloudwatch.statistics("AWS/S3", metric, Map("BucketName" -> bucket.value, "StorageType" -> storage), "Average", since, now, 86400).map(_.lastOption.getOrElse(0d))
                      (for
                        bytes     <- latest("BucketSizeBytes", "StandardStorage")
                        objects   <- latest("NumberOfObjects", "AllStorageTypes")
                        lifecycle <- s3.getBucketLifecycle(bucket).mapError(e => AwsError.InvalidRequest(e.message))
                      yield
                        val gb      = bytes / 1e9
                        val savings = gb * 0.5 * (book("s3.gb-month") - book("s3.ia-gb-month")) - objects / 1000 * book("s3.tiering-1000-objects")
                        Option.when(gb >= minGb && lifecycle.tieringAfterDays.isEmpty && savings > 0):
                          StorageFinding(
                            d.address,
                            f"$gb%.1f GB in S3 Standard and no Intelligent-Tiering",
                            "add `lifecycle = Some(Lifecycle(tieringAfterDays = Some(0)))` to its `BucketSpec`",
                            math.round(savings * 100) / 100d,
                          )
                      ).mapError(e => ProviderError(d.address, "storage advice", e.message))
    yield findings.flatten.flatten

/** Compute Optimizer's right-sizing and the storage advice, together. */
final case class AwsRecommendations(rightSizing: ComputeOptimizerRecommendations, storageAdvice: StorageAdvice) extends Recommendations:
  def services(app: AppName, env: EnvName): IO[ProviderError, List[RightSizing]] = rightSizing.services(app, env)

  override def storage(app: AppName, env: EnvName): IO[ProviderError, List[StorageFinding]] = storageAdvice.storage(app, env)
