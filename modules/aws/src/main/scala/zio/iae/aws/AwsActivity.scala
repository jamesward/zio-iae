package zio.iae.aws

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** How much each managed resource is used, from its CloudWatch metrics over the window (daily sums):
  *   - a queue: the messages sent to it; idle with none;
  *   - a topic: the messages published (SNS doesn't say when a topic was made, so never called idle);
  *   - a table: the read and write capacity consumed; idle with none;
  *   - a service: the requests its load balancer served; idle with none;
  *   - a bucket: what it stores (its daily storage metrics). S3's request metrics are off by default, so a bucket
  *     is never called idle from them; its size is context for people deciding.
  *
  * A resource younger than the window, or whose age isn't known, is never idle: it hasn't had the time (its creation
  * time comes from SQS, DynamoDB or the load balancer).
  */
final case class AwsActivity(cloudwatch: CloudWatchClient, elb: Elbv2Client, sqs: SqsClient, dynamo: DynamoClient, buckets: BucketNaming) extends Activity:
  def usage(address: Address, since: Instant): IO[ProviderError, Option[ResourceUsage]] =
    Clock.instant.flatMap: now =>
      val days = java.time.Duration.between(since, now).nn.toDays.max(1)
      def sum(namespace: String, metric: String, dims: Map[String, String]): IO[AwsError, Double] =
        cloudwatch.statistics(namespace, metric, dims, "Sum", since, now, 86400).map(_.sum)
      def counted(n: Double, what: String, created: Option[Instant]): ResourceUsage =
        val young = created.exists(_.isAfter(since)) || created.isEmpty
        val text  = if n == 0 then s"no $what in $days days" else s"${AwsActivity.count(n)} $what in $days days"
        ResourceUsage(address, n == 0 && !young, if young && n == 0 then s"$text (but it's newer than that)" else text)
      val found: IO[AwsError, Option[ResourceUsage]] = address.kind match
        case ResourceKind.Queue =>
          ZIO.foreach(QueueNaming.physical(address).toOption): q =>
            for
              url     <- sqs.queueUrl(q)
              attrs   <- ZIO.foreach(url)(sqs.attributes).map(_.getOrElse(Map.empty))
              created  = attrs.get("CreatedTimestamp").flatMap(_.toLongOption).map(Instant.ofEpochSecond(_).nn)
              sent    <- sum("AWS/SQS", "NumberOfMessagesSent", Map("QueueName" -> q))
            yield counted(sent, "messages sent", created)
        case ResourceKind.Topic =>
          sum("AWS/SNS", "NumberOfMessagesPublished", Map("TopicName" -> TopicNaming.physical(address))).map(n => Some(counted(n, "messages published", None)))
        case ResourceKind.Table =>
          val table = Map("TableName" -> TableNaming.physical(address))
          for
            described <- dynamo.describe(TableNaming.physical(address))
            reads     <- sum("AWS/DynamoDB", "ConsumedReadCapacityUnits", table)
            writes    <- sum("AWS/DynamoDB", "ConsumedWriteCapacityUnits", table)
          yield
            if reads + writes == 0 then Some(counted(0, "reads or writes", described.flatMap(_.created)))
            else Some(ResourceUsage(address, false, s"${AwsActivity.count(reads)} read units and ${AwsActivity.count(writes)} write units in $days days"))
        case ResourceKind.Service =>
          elb.loadBalancer(ServiceNaming.loadBalancer(address)).flatMap: lb =>
            ZIO.foreach(lb)(l => sum("AWS/ApplicationELB", "RequestCount", Map("LoadBalancer" -> ServiceProvider.arnSuffix(l.arn, "loadbalancer/"))).map(counted(_, "requests", l.created)))
        case ResourceKind.Bucket =>
          ZIO.foreach(buckets.physical(address).toOption): bucket =>
            def latest(metric: String, storage: String) =
              cloudwatch.statistics("AWS/S3", metric, Map("BucketName" -> bucket.value, "StorageType" -> storage), "Average", since, now, 86400).map(_.lastOption)
            (latest("BucketSizeBytes", "StandardStorage") <*> latest("NumberOfObjects", "AllStorageTypes")).map:
              case (None, None) => ResourceUsage(address, idle = false, "no storage metrics yet")
              case (bytes, objects) =>
                ResourceUsage(address, idle = false, s"${AwsActivity.bytes(bytes.getOrElse(0d))} in ${AwsActivity.count(objects.getOrElse(0d))} objects")
        case _ => ZIO.none
      found.mapError(e => ProviderError(address, "read usage", e.message))

object AwsActivity:
  def count(n: Double): String = String.format(java.util.Locale.ROOT, "%,d", math.round(n))

  def bytes(n: Double): String =
    if n >= 1e9 then f"${n / 1e9}%.1f GB" else if n >= 1e6 then f"${n / 1e6}%.1f MB" else if n >= 1e3 then f"${n / 1e3}%.1f KB" else s"${math.round(n)} bytes"
