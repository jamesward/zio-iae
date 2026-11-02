package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** The alarms every queue and table gets, whatever its spec: written when the resource is applied, removed when it's
  * deleted. They aren't part of the spec, so adding or tuning them doesn't change any resource's spec hash.
  */
object DefaultAlarms:
  private val period = 300

  def prefix(address: Address): String = s"iae-${address.app.value}-${address.env.value}-${address.id.value}-"

  /** What alarms an address has, and how an alert names them. */
  def of(address: Address, spec: ResourceSpec): List[Gate] =
    val p = prefix(address)
    spec match
      case ResourceSpec.Queue(queue) =>
        val main = QueueNaming.physical(address).toOption.toList.map: name =>
          Gate(MetricAlarm(s"${p}age", "AWS/SQS", "ApproximateAgeOfOldestMessage", Map("QueueName" -> name), "Maximum", period, 1, 900, "GreaterThanThreshold"), "age")
        val dead = queue.deadLetter.flatMap(_ => QueueNaming.deadLetter(address).toOption).toList.map: dlq =>
          Gate(MetricAlarm(s"${p}dead-letters", "AWS/SQS", "ApproximateNumberOfMessagesVisible", Map("QueueName" -> dlq), "Maximum", period, 1, 1, "GreaterThanOrEqualToThreshold"), "dead letters")
        main ++ dead
      case ResourceSpec.Table(_) =>
        val table = Map("TableName" -> TableNaming.physical(address))
        List(
          Gate(MetricAlarm(s"${p}read-throttles", "AWS/DynamoDB", "ReadThrottleEvents", table, "Sum", period, 1, 1, "GreaterThanOrEqualToThreshold"), "read throttles"),
          Gate(MetricAlarm(s"${p}write-throttles", "AWS/DynamoDB", "WriteThrottleEvents", table, "Sum", period, 1, 1, "GreaterThanOrEqualToThreshold"), "write throttles"),
        )
      case _ => Nil

  /** How an alert names a default alarm, from its name. */
  def gate(address: Address, alarm: String): String =
    alarm.stripPrefix(prefix(address)).replace('-', ' ')

  /** Writes the address's default alarms and removes the ones it no longer has (a dead-letter queue taken away). */
  def put(cloudwatch: CloudWatchClient, address: Address, spec: ResourceSpec): IO[AwsError, Unit] =
    val gates = of(address, spec)
    for
      _     <- ZIO.foreachDiscard(gates)(g => cloudwatch.putAlarm(g.alarm))
      names <- cloudwatch.alarmNames(prefix(address))
      _     <- cloudwatch.deleteAlarms(names.filter(n => known(address, n) && !gates.exists(_.alarm.name == n)))
    yield ()

  def remove(cloudwatch: CloudWatchClient, address: Address): IO[AwsError, Unit] =
    cloudwatch.alarmNames(prefix(address)).flatMap(names => cloudwatch.deleteAlarms(names.filter(known(address, _))))

  /** Only ours: a service with the same id has its own alarms under the same prefix. */
  def known(address: Address, name: String): Boolean =
    Set("age", "dead-letters", "read-throttles", "write-throttles").contains(name.stripPrefix(prefix(address)))
