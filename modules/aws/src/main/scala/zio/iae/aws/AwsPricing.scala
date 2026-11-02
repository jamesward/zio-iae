package zio.iae.aws

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Unit prices in USD. */
final case class PriceBook(region: String, prices: Map[String, Double]):
  def apply(unit: String): Double = prices.getOrElse(unit, 0d)

object PriceBook:
  val hoursPerMonth = 730d

  /** AWS's public on-demand list prices in us-east-1 (2025). Real accounts are to read the Price List API, cached
    * daily, with this as the fallback.
    */
  val usEast1: PriceBook = PriceBook(
    "us-east-1",
    Map(
      "fargate.vcpu-hour"           -> 0.04048,
      "fargate.gb-hour"             -> 0.004445,
      "alb.hour"                    -> 0.0225,
      "alb.lcu-hour"                -> 0.008,
      "cloudwatch.alarm-month"      -> 0.10,
      "s3.gb-month"                 -> 0.023,
      "sqs.million-requests"        -> 0.40,
      "sns.million-publishes"       -> 0.50,
      "dynamodb.million-writes"     -> 0.625,
      "dynamodb.million-reads"      -> 0.125,
      "secretsmanager.secret-month" -> 0.40,
    ),
  )

/** What a change costs a month, from its spec and the resource's last 30 days of CloudWatch metrics:
  *   - a service: its tasks (vCPU and memory × count × hours), its load balancer (hours and one LCU) and its alarms;
  *     an update compares with the size the live version recorded (`iae:capacity`);
  *   - a secret: its monthly price; a role: nothing;
  *   - a bucket, queue, topic or table: what its storage or traffic was, which a spec change doesn't change, so an
  *     update costs nothing more; a new one has no usage yet, which its assumptions say.
  */
final case class AwsPricing(book: PriceBook, cloudwatch: CloudWatchClient, elb: Elbv2Client, buckets: BucketNaming) extends Pricing:
  import AwsPricing.*

  def estimate(planned: PlannedChange): IO[ProviderError, Option[CostDelta]] =
    planned.change match
      case Change.Create(address, spec) => monthly(address, spec, existing = false).map(c => Some(CostDelta(0, c.amount, c.exact, c.assumptions)))
      case Change.Update(address, spec, _) =>
        for
          after  <- monthly(address, spec, existing = true)
          before <- previous(address).map(_.getOrElse(after))
        yield Some(CostDelta(before.amount, after.amount, after.exact && before.exact, (before.assumptions ++ after.assumptions).distinct))
      case Change.Delete(address) =>
        previous(address).map(_.map(c => CostDelta(c.amount, 0, c.exact, c.assumptions)))
      case Change.Orphan(_, _) | Change.Rollback(_, _, _) | Change.Prune(_, _) => ZIO.none

  /** The monthly cost of a resource as the spec declares it. */
  def monthly(address: Address, spec: ResourceSpec, existing: Boolean): IO[ProviderError, Cost] =
    spec match
      case ResourceSpec.Service(s) => ZIO.succeed(service(s.cpu, s.memory, s.count, ServiceProvider.gateCount(s.canary)))
      case ResourceSpec.Secret(_)  => ZIO.succeed(Cost(book("secretsmanager.secret-month"), exact = true))
      case ResourceSpec.Role(_)    => ZIO.succeed(Cost(0, exact = true))
      case ResourceSpec.Bucket(_)  => usage(address, existing)
      case ResourceSpec.Queue(q)   => usage(address, existing).map(c => c.plus(alarms(if q.deadLetter.isDefined then 2 else 1)))
      case ResourceSpec.Topic(_)   => usage(address, existing)
      case ResourceSpec.Table(_)   => usage(address, existing).map(c => c.plus(alarms(2)))

  /** What the resource costs now, for an update's "before" or a deletion. */
  private def previous(address: Address): IO[ProviderError, Option[Cost]] =
    address.kind match
      case ResourceKind.Service =>
        elb.loadBalancer(ServiceNaming.loadBalancer(address)).flatMap(ZIO.foreach(_)(lb => elb.tags(lb.arn)))
          .mapBoth(failed(address), _.flatMap(_.get(ServiceProvider.CapacityTag)).flatMap(Capacity.parse).map(c => service(c.cpu, c.memory, c.count, c.alarms)))
      case ResourceKind.Secret => ZIO.some(Cost(book("secretsmanager.secret-month"), exact = true))
      case ResourceKind.Role   => ZIO.some(Cost(0, exact = true))
      case _                   => usage(address, existing = true).map(Some(_))

  private def service(cpu: Int, memory: Int, count: Int, gates: Int): Cost =
    val tasks = (cpu / 1024d * book("fargate.vcpu-hour") + memory / 1024d * book("fargate.gb-hour")) * count * PriceBook.hoursPerMonth
    val lb    = (book("alb.hour") + book("alb.lcu-hour")) * PriceBook.hoursPerMonth
    Cost(tasks + lb, exact = false, List("the load balancer's traffic is one LCU")).plus(alarms(gates * 2))

  private def alarms(count: Int): Cost = Cost(count * book("cloudwatch.alarm-month"), exact = true)

  /** Storage or traffic over the last 30 days, scaled to a month. */
  private def usage(address: Address, existing: Boolean): IO[ProviderError, Cost] =
    if !existing then ZIO.succeed(Cost(0, exact = false, List(s"a new ${address.kind.toString.toLowerCase} has no usage yet")))
    else
      Clock.instant.flatMap: now =>
        val since = now.minusSeconds(30L * 24 * 3600).nn
        def sum(namespace: String, metric: String, dims: Map[String, String]) =
          cloudwatch.statistics(namespace, metric, dims, "Sum", since, now, 86400).map(_.sum)
        val cost: IO[AwsError, Cost] = address.kind match
          case ResourceKind.Bucket =>
            ZIO.foreach(buckets.physical(address).toOption): bucket =>
              cloudwatch.statistics("AWS/S3", "BucketSizeBytes", Map("BucketName" -> bucket.value, "StorageType" -> "StandardStorage"), "Average", since, now, 86400)
                .map(_.lastOption.getOrElse(0d) / 1e9 * book("s3.gb-month"))
            .map(c => Cost(c.getOrElse(0d), exact = false, List("S3 requests aren't counted (request metrics are off by default)")))
          case ResourceKind.Queue =>
            ZIO.foreach(QueueNaming.physical(address).toOption)(q => sum("AWS/SQS", "NumberOfMessagesSent", Map("QueueName" -> q)))
              .map(sent => Cost(sent.getOrElse(0d) * 3 / 1e6 * book("sqs.million-requests"), exact = false, List("each message is sent, received and deleted")))
          case ResourceKind.Topic =>
            sum("AWS/SNS", "NumberOfMessagesPublished", Map("TopicName" -> TopicNaming.physical(address)))
              .map(n => Cost(n / 1e6 * book("sns.million-publishes"), exact = false))
          case ResourceKind.Table =>
            val table = Map("TableName" -> TableNaming.physical(address))
            (sum("AWS/DynamoDB", "ConsumedReadCapacityUnits", table) <*> sum("AWS/DynamoDB", "ConsumedWriteCapacityUnits", table)).map: (reads, writes) =>
              Cost(reads / 1e6 * book("dynamodb.million-reads") + writes / 1e6 * book("dynamodb.million-writes"), exact = false, List("storage isn't counted"))
          case _ => ZIO.succeed(Cost(0, exact = true))
        cost.mapError(failed(address))

  private def failed(address: Address)(e: AwsError): ProviderError = ProviderError(address, "estimate cost", e.message)

object AwsPricing:
  final case class Cost(amount: Double, exact: Boolean, assumptions: List[String] = Nil):
    def plus(other: Cost): Cost = Cost(amount + other.amount, exact && other.exact, (assumptions ++ other.assumptions).distinct)

  /** What a service's live version runs, recorded on its load balancer (`cpu/memory/count/gates`). */
  final case class Capacity(cpu: Int, memory: Int, count: Int, alarms: Int) derives CanEqual:
    def encode: String = s"$cpu/$memory/$count/$alarms"

  object Capacity:
    def parse(value: String): Option[Capacity] =
      value.split('/').toList.map(_.toIntOption) match
        case List(Some(c), Some(m), Some(n), Some(a)) => Some(Capacity(c, m, n, a))
        case _                                        => None
