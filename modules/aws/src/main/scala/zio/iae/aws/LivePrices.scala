package zio.iae.aws

import java.time.Instant
import zio.*
import zio.iae.aws.client.*
import zio.json.*
import zio.json.ast.Json

/** Where plans' and recommendations' unit prices come from. */
trait Prices:
  def book: UIO[PriceBook]

object Prices:
  def fixed(book: PriceBook): Prices =
    val fixed = book
    new Prices:
      def book: UIO[PriceBook] = ZIO.succeed(fixed)

/** The env's region's on-demand prices from the Price List API, read once a day. A unit the API doesn't answer for
  * (or a failed read, retried after an hour) keeps the bundled price, which is us-east-1's: close for most regions,
  * and said in the log.
  */
final case class LivePrices(client: PriceListClient, region: String, fallback: PriceBook, cache: Ref.Synchronized[Option[(Instant, PriceBook)]], ttl: Duration = 1.day)
    extends Prices:
  def book: UIO[PriceBook] =
    cache.modifyZIO: cached =>
      Clock.instant.flatMap: now =>
        cached match
          case Some((until, book)) if now.isBefore(until) => ZIO.succeed((book, cached))
          case _ =>
            load.foldZIO(
              e => ZIO.logWarning(s"reading prices for $region from the Price List API: ${e.message}; using the bundled ones").as((fallback, Some((now.plus(1.hour).nn, fallback)))),
              book => ZIO.succeed((book, Some((now.plus(ttl).nn, book)))),
            )

  private def load: IO[AwsError, PriceBook] =
    ZIO
      .foreach(LivePrices.queries): q =>
        client.products(q.service, List(("TERM_MATCH", "regionCode", region), ("CONTAINS", "usagetype", q.usage)) ++ q.family.map(f => ("TERM_MATCH", "productFamily", f)).toList)
          .map(products => q.unit -> LivePrices.price(products, q))
      .flatMap: found =>
        val missing = found.collect { case (unit, None) => unit }
        ZIO.logInfo(s"no Price List price in $region for ${missing.mkString(", ")}; using the bundled ones").when(missing.nonEmpty)
          .as(PriceBook(region, fallback.prices ++ found.collect { case (unit, Some(p)) => unit -> p }))

object LivePrices:
  def make(client: PriceListClient, region: String, fallback: PriceBook = PriceBook.usEast1): LivePrices =
    Unsafe.unsafe(implicit u => LivePrices(client, region, fallback, Ref.Synchronized.unsafe.make(Option.empty[(Instant, PriceBook)])))

  /** One price-book unit: the service code, the usage type it ends with, and what one priced unit is worth in it
    * (e.g. a million requests).
    */
  final case class Query(unit: String, service: String, usage: String, family: Option[String] = None, scale: Double = 1)

  val queries: List[Query] = List(
    Query("fargate.vcpu-hour", "AmazonECS", "Fargate-vCPU-Hours:perCPU"),
    Query("fargate.gb-hour", "AmazonECS", "Fargate-GB-Hours"),
    Query("alb.hour", "AWSELB", "LoadBalancerUsage", Some("Load Balancer-Application")),
    Query("alb.lcu-hour", "AWSELB", "LCUUsage", Some("Load Balancer-Application")),
    Query("cloudwatch.alarm-month", "AmazonCloudWatch", "CW:AlarmMonitorUsage"),
    Query("s3.gb-month", "AmazonS3", "TimedStorage-ByteHrs"),
    Query("s3.ia-gb-month", "AmazonS3", "TimedStorage-INT-IA-ByteHrs"),
    Query("s3.tiering-1000-objects", "AmazonS3", "Monitoring-Automation-INT", scale = 1000),
    Query("sqs.million-requests", "AWSQueueService", "Requests-Tier1", scale = 1e6),
    Query("sns.million-publishes", "AmazonSNS", "Requests-Tier1", scale = 1e6),
    Query("dynamodb.million-writes", "AmazonDynamoDB", "WriteRequestUnits", scale = 1e6),
    Query("dynamodb.million-reads", "AmazonDynamoDB", "ReadRequestUnits", scale = 1e6),
    Query("secretsmanager.secret-month", "AWSSecretsManager", "AWSSecretsManager-Secrets"),
  )

  /** The query's price among the products: those whose usage type ends with it, on-demand, the first paid tier
    * (lowest begin range with a price above zero, past any free tier); the lowest of them.
    */
  def price(products: List[String], q: Query): Option[Double] =
    def str(j: Option[Json]): Option[String] = j.flatMap(_.asString)
    val prices = products.flatMap(_.fromJson[Json.Obj].toOption).flatMap: product =>
      val attributes = product.get("product").flatMap(_.asObject).flatMap(_.get("attributes")).flatMap(_.asObject)
      val usage      = str(attributes.flatMap(_.get("usagetype")))
      val family     = str(product.get("product").flatMap(_.asObject).flatMap(_.get("productFamily")))
      if !usage.exists(_.endsWith(q.usage)) || q.family.exists(f => !family.contains(f)) then Nil
      else
        val dimensions = for
          terms     <- product.get("terms").flatMap(_.asObject).flatMap(_.get("OnDemand")).flatMap(_.asObject).toList
          (_, term) <- terms.fields.toList
          dims      <- term.asObject.flatMap(_.get("priceDimensions")).flatMap(_.asObject).toList
          (_, dim)  <- dims.fields.toList
          d         <- dim.asObject.toList
          usd       <- str(d.get("pricePerUnit").flatMap(_.asObject).flatMap(_.get("USD"))).flatMap(_.toDoubleOption).toList
          begin      = str(d.get("beginRange")).flatMap(_.toDoubleOption).getOrElse(0d)
        yield (begin, usd)
        dimensions.filter(_._2 > 0).minByOption(_._1).map(_._2 * q.scale).toList
    prices.minOption
