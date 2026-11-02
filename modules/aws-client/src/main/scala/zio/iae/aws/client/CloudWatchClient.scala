package zio.iae.aws.client

import scala.xml.Elem
import zio.*

enum AlarmState derives CanEqual:
  case Ok, Alarm, InsufficientData

  def value: String = this match
    case Ok               => "OK"
    case Alarm            => "ALARM"
    case InsufficientData => "INSUFFICIENT_DATA"

object AlarmState:
  def from(value: String): Option[AlarmState] = values.find(_.value == value)

/** A metric alarm: a threshold on a statistic of a metric over evaluation periods. A percentile statistic (`p99`,
  * `p99.9`) is sent as CloudWatch's extended statistic.
  */
final case class MetricAlarm(
    name: String,
    namespace: String,
    metric: String,
    dimensions: Map[String, String],
    statistic: String,
    periodSeconds: Int,
    evaluationPeriods: Int,
    threshold: Double,
    comparison: String,
)

/** CloudWatch metrics and alarms: what the canary's gates read, and what the harness's traffic generator writes. */
final case class CloudWatchClient(query: AwsQueryClient):
  import CloudWatchClient.*

  def putMetric(namespace: String, metric: String, value: Double, dimensions: Map[String, String]): IO[AwsError, Unit] =
    val dims = dimensions.toList.sorted.zipWithIndex.flatMap((kv, i) =>
      List(s"MetricData.member.1.Dimensions.member.${i + 1}.Name" -> kv._1, s"MetricData.member.1.Dimensions.member.${i + 1}.Value" -> kv._2))
    call("PutMetricData", List("Namespace" -> namespace, "MetricData.member.1.MetricName" -> metric, "MetricData.member.1.Value" -> value.toString) ++ dims).unit

  def putAlarm(alarm: MetricAlarm): IO[AwsError, Unit] =
    val dims = alarm.dimensions.toList.sorted.zipWithIndex.flatMap((kv, i) => List(s"Dimensions.member.${i + 1}.Name" -> kv._1, s"Dimensions.member.${i + 1}.Value" -> kv._2))
    call(
      "PutMetricAlarm",
      List(
        "AlarmName" -> alarm.name, "Namespace" -> alarm.namespace, "MetricName" -> alarm.metric,
        (if CloudWatchClient.percentile(alarm.statistic) then "ExtendedStatistic" else "Statistic") -> alarm.statistic,
        "Period" -> alarm.periodSeconds.toString, "EvaluationPeriods" -> alarm.evaluationPeriods.toString,
        "Threshold" -> alarm.threshold.toString, "ComparisonOperator" -> alarm.comparison, "TreatMissingData" -> "notBreaching",
      ) ++ dims,
    ).unit

  def states(names: List[String]): IO[AwsError, Map[String, AlarmState]] =
    if names.isEmpty then ZIO.succeed(Map.empty)
    else
      call("DescribeAlarms", names.zipWithIndex.map((n, i) => s"AlarmNames.member.${i + 1}" -> n)).map: e =>
        (e \\ "MetricAlarms" \ "member").toList.flatMap(m => AlarmState.from((m \ "StateValue").text.trim).map((m \ "AlarmName").text.trim -> _)).toMap

  def setState(name: String, state: AlarmState, reason: String): IO[AwsError, Unit] =
    call("SetAlarmState", List("AlarmName" -> name, "StateValue" -> state.value, "StateReason" -> reason)).unit

  /** The names of the alarms whose names start with the prefix. */
  def alarmNames(prefix: String): IO[AwsError, List[String]] =
    call("DescribeAlarms", List("AlarmNamePrefix" -> prefix, "MaxRecords" -> "100")).map(e => (e \\ "MetricAlarms" \ "member").toList.map(m => (m \ "AlarmName").text.trim))

  def deleteAlarms(names: List[String]): IO[AwsError, Unit] =
    call("DeleteAlarms", names.zipWithIndex.map((n, i) => s"AlarmNames.member.${i + 1}" -> n)).unit.when(names.nonEmpty).unit

  private def call(action: String, params: List[(String, String)]): IO[AwsError, Elem] = query.call(protocol, action, params)

object CloudWatchClient:
  def percentile(statistic: String): Boolean = statistic.matches("p\\d+(\\.\\d+)?")

  val layer: ZLayer[AwsQueryClient, Nothing, CloudWatchClient] = ZLayer.fromFunction(CloudWatchClient.apply)

  private val protocol = AwsQueryProtocol(AwsService("monitoring", "monitoring"), "2010-08-01")
