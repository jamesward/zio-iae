package zio.iae

import zio.*
import zio.schema.{Schema, derived}

/** Gates the canary on the new version's response time, as the load balancer measures it: more than `millis` for the
  * statistic (`p99` by default; any CloudWatch statistic, e.g. `Average`) over a period sets off the alarm.
  */
final case class LatencyGate(millis: Int, statistic: String = "p99") derives Schema, CanEqual

/** An alarm on a metric the app publishes itself, which gates the canary like the load balancer's. The app publishes
  * `metric` to its namespace (`IAE_METRICS_NAMESPACE`, `iae/<app>`) with the dimensions `Service` (`IAE_SERVICE`) and
  * `Color` (`IAE_COLOR`), so each version is judged on its own numbers. It goes off when the statistic over a period
  * reaches `threshold`, or with `below`, when it falls to it. `name` is short: lowercase letters, digits and dashes.
  */
final case class AppAlarm(name: String, metric: String, threshold: Double, statistic: String = "Sum", below: Boolean = false) derives Schema, CanEqual

/** How a new version takes traffic: the share (in percent) the new version gets at each step, how long each step
  * bakes while its alarms are watched, and the alarms' period (60 seconds for the load balancer's metrics). The last
  * step should be 100. A step rolls back when the new version answers `maxErrors` 5xx in a period, is slower than its
  * `latency` gate, or sets off one of the app's own `alarms`.
  */
final case class CanarySpec(
    steps: List[Int] = List(10, 50, 100),
    bakeSeconds: Int = 300,
    alarmPeriodSeconds: Int = 60,
    maxErrors: Int = 1,
    latency: Option[LatencyGate] = None,
    alarms: List[AppAlarm] = Nil,
) derives Schema, CanEqual

/** The app running as a web service: containers of its image behind a load balancer, deployed by canary.
  * `environment` and `command` are passed to the container as they are. `image` is filled in by the agent after a
  * build (from the app's container in `.iae/config.json`), so a new commit's image is a change to the service like
  * any other.
  */
final case class ServiceSpec(
    port: Int,
    healthPath: String = "/health",
    cpu: Int = 256,
    memory: Int = 512,
    count: Int = 1,
    listenerPort: Int = 80,
    canary: CanarySpec = CanarySpec(),
    environment: Map[String, String] = Map.empty,
    command: List[String] = Nil,
    image: Option[String] = None,
) derives Schema, CanEqual

object Service:
  /** {{{ object Web extends Service.Id(LogicalId("web"), ServiceSpec(port = 8080)) }}} */
  abstract class Id(val logicalId: LogicalId, val spec: ServiceSpec)

  /** Declares that the app runs as this service. It gives the program nothing to call: the program is the service.
    * A service with alarms of its own may publish their metrics.
    */
  def run[I <: Id & Singleton](id: I): Infra[Any] =
    val publishes = Chunk.fromIterable(Option.when(id.spec.canary.alarms.nonEmpty)(Grant(id.logicalId, ResourceKind.Service, Capability.MetricsPublish)))
    Infra(resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Service(id.spec))), grants = publishes, layer = ZLayer.empty)

  /** The namespace an app's own metrics go to. */
  def metricsNamespace(app: AppName): String = s"iae/${app.value}"

  private val alarmName = "[a-z0-9][a-z0-9-]{0,31}".r

  /** Why the spec can't be deployed, if it can't. */
  def invalid(spec: ServiceSpec): Option[String] =
    val names = spec.canary.alarms.map(_.name)
    names.find(n => !alarmName.matches(n)).map(n => s"alarm name '$n' must be lowercase letters, digits and dashes (at most 32)")
      .orElse(Option.when(names.distinct.size != names.size)(s"alarm names must be distinct: ${names.mkString(", ")}"))
      .orElse(Option.when(spec.canary.maxErrors < 1)("maxErrors must be at least 1"))
      .orElse(spec.canary.latency.filter(_.millis < 1).map(_ => "the latency gate must be at least 1 ms"))
