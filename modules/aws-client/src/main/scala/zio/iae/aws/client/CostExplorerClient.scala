package zio.iae.aws.client

import java.time.LocalDate
import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

/** What resources with the given tags cost over a period: in USD, in total and by AWS service. */
final case class TaggedCost(from: LocalDate, to: LocalDate, total: Double, byService: Map[String, Double])

/** A spike Cost Anomaly Detection found: its id, when it started, what it's about (the monitor's dimension, e.g. the
  * service), how much more it cost than expected (USD) and the root causes it names.
  */
final case class CostAnomalyFound(id: String, start: Option[LocalDate], dimension: Option[String], impact: Double, causes: List[String])

/** Cost Explorer: the bill by cost-allocation tag (`GetCostAndUsage`), and Cost Anomaly Detection's findings
  * (`GetAnomalies`). One endpoint in us-east-1 for every region.
  */
final case class CostExplorerClient(json: AwsJsonClient):
  import CostExplorerClient.*

  /** The unblended cost of what carries all the tags, `from` inclusive to `to` exclusive. */
  def costs(tags: Map[String, String], from: LocalDate, to: LocalDate): IO[AwsError, TaggedCost] =
    val leaves = tags.toList.sorted.map((k, v) => Leaf(Some(TagValues(k, List(v), Some(List("EQUALS"))))))
    val filter = leaves match
      case Nil         => None
      case List(one)   => Some(Expression(None, one.tags))
      case many        => Some(Expression(Some(many), None))
    def page(token: Option[String]): IO[AwsError, List[Result]] =
      json
        .call[CostRequest, CostResponse](protocol, "GetCostAndUsage",
          CostRequest(DateRange(from.toString, to.toString), "MONTHLY", List(Metric), filter, List(GroupDefinition("DIMENSION", "SERVICE")), token))
        .flatMap(r => r.next.filter(_.nonEmpty).fold(ZIO.succeed(r.results.getOrElse(Nil)))(t => page(Some(t)).map(r.results.getOrElse(Nil) ++ _)))
    page(None).map: results =>
      val byService = results.flatMap(_.groups.getOrElse(Nil)).flatMap(g => g.keys.headOption.map(_ -> amount(g.metrics)))
        .groupMapReduce(_._1)(_._2)(_ + _)
      val totals = results.flatMap(_.total).map(amount).sum
      TaggedCost(from, to, if byService.nonEmpty then byService.values.sum else totals, byService)

  /** The anomalies detected from `from` on. */
  def anomalies(from: LocalDate, to: LocalDate): IO[AwsError, List[CostAnomalyFound]] =
    def page(token: Option[String]): IO[AwsError, List[AnomalyJson]] =
      json
        .call[AnomaliesRequest, AnomaliesResponse](protocol, "GetAnomalies", AnomaliesRequest(DateInterval(from.toString, Some(to.toString)), token, 100))
        .flatMap(r => r.next.filter(_.nonEmpty).fold(ZIO.succeed(r.anomalies.getOrElse(Nil)))(t => page(Some(t)).map(r.anomalies.getOrElse(Nil) ++ _)))
    page(None).map(_.map: a =>
      CostAnomalyFound(
        a.id,
        a.start.flatMap(s => scala.util.Try(LocalDate.parse(s.take(10)).nn).toOption),
        a.dimension,
        a.impact.flatMap(i => i.total.orElse(i.max)).getOrElse(0d),
        a.causes.getOrElse(Nil).map(c => List(c.service, c.usageType, c.region).flatten.mkString(" / ")).filter(_.nonEmpty),
      ))

object CostExplorerClient:
  val layer: ZLayer[AwsJsonClient, Nothing, CostExplorerClient] = ZLayer.fromFunction(CostExplorerClient.apply)

  /** `ce.us-east-1.amazonaws.com`, signed for us-east-1 whatever the config's region. */
  private val protocol = AwsJsonProtocol(AwsService("ce", "ce.us-east-1", global = true), "AWSInsightsIndexService", "1.1")

  private val Metric = "UnblendedCost"

  private def amount(m: Metrics): Double = m.unblended.flatMap(_.amount.toDoubleOption).getOrElse(0d)

  final case class DateRange(@fieldName("Start") start: String, @fieldName("End") end: String) derives Schema
  final case class TagValues(@fieldName("Key") key: String, @fieldName("Values") values: List[String], @fieldName("MatchOptions") matchOptions: Option[List[String]])
      derives Schema
  final case class Leaf(@fieldName("Tags") tags: Option[TagValues]) derives Schema
  final case class Expression(@fieldName("And") and: Option[List[Leaf]], @fieldName("Tags") tags: Option[TagValues]) derives Schema
  final case class GroupDefinition(@fieldName("Type") tpe: String, @fieldName("Key") key: String) derives Schema
  final case class CostRequest(
      @fieldName("TimePeriod") period: DateRange,
      @fieldName("Granularity") granularity: String,
      @fieldName("Metrics") metrics: List[String],
      @fieldName("Filter") filter: Option[Expression],
      @fieldName("GroupBy") groupBy: List[GroupDefinition],
      @fieldName("NextPageToken") next: Option[String],
  ) derives Schema
  final case class MetricValue(@fieldName("Amount") amount: String, @fieldName("Unit") unit: Option[String]) derives Schema
  final case class Metrics(@fieldName("UnblendedCost") unblended: Option[MetricValue]) derives Schema
  final case class Group(@fieldName("Keys") keys: List[String], @fieldName("Metrics") metrics: Metrics) derives Schema
  final case class Result(@fieldName("Total") total: Option[Metrics], @fieldName("Groups") groups: Option[List[Group]]) derives Schema
  final case class CostResponse(@fieldName("ResultsByTime") results: Option[List[Result]], @fieldName("NextPageToken") next: Option[String]) derives Schema

  final case class DateInterval(@fieldName("StartDate") start: String, @fieldName("EndDate") end: Option[String]) derives Schema
  final case class AnomaliesRequest(@fieldName("DateInterval") interval: DateInterval, @fieldName("NextPageToken") next: Option[String], @fieldName("MaxResults") max: Int)
      derives Schema
  final case class RootCause(@fieldName("Service") service: Option[String], @fieldName("Region") region: Option[String], @fieldName("UsageType") usageType: Option[String])
      derives Schema
  final case class Impact(@fieldName("TotalImpact") total: Option[Double], @fieldName("MaxImpact") max: Option[Double]) derives Schema
  final case class AnomalyJson(
      @fieldName("AnomalyId") id: String,
      @fieldName("AnomalyStartDate") start: Option[String],
      @fieldName("DimensionValue") dimension: Option[String],
      @fieldName("RootCauses") causes: Option[List[RootCause]],
      @fieldName("Impact") impact: Option[Impact],
  ) derives Schema
  final case class AnomaliesResponse(@fieldName("Anomalies") anomalies: Option[List[AnomalyJson]], @fieldName("NextPageToken") next: Option[String]) derives Schema
