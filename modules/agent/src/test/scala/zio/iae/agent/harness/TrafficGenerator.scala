package zio.iae.agent.harness

import zio.*
import zio.http.{Client, Request, URL}
import zio.iae.*
import zio.iae.aws.{Color, ServiceNaming, ServiceProvider}
import zio.iae.aws.client.*

/** Steady requests to every task of the app's services, in every env, standing in for users. A real ALB publishes
  * each target group's 5xx count and response time; Floci doesn't, so this publishes them the same way
  * (`AWS/ApplicationELB`, `HTTPCode_Target_5XX_Count` and `TargetResponseTime` by load balancer and target group), and
  * a canary's alarms go off on real responses. The env's faults are added to the version in canary (the color that
  * isn't live), including the app's own failure metric.
  */
final case class TrafficGenerator(http: Client, app: AppName, service: LogicalId, envs: List[(EnvName, AwsConfig)], faults: Faults):
  def run: UIO[Nothing] =
    ZIO.foreachDiscard(envs)((env, config) => once(env, config).catchAll(e => ZIO.logDebug(s"traffic in ${env.value}: $e")))
      .repeat(Schedule.spaced(2.seconds))
      .withClock(Clock.ClockLive)
      .forever

  private def once(env: EnvName, config: AwsConfig): IO[String, Unit] =
    val query   = AwsQueryClient(http, config)
    val elb     = Elbv2Client(query)
    val cw      = CloudWatchClient(query)
    val address = Address(app, env, ResourceKind.Service, service)
    elb.loadBalancer(ServiceNaming.loadBalancer(address)).mapError(_.message).flatMap:
      case None => ZIO.unit
      case Some(lb) =>
        for
          tags   <- elb.tags(lb.arn).mapError(_.message)
          live    = tags.get(ServiceProvider.ColorTag).flatMap(Color.from)
          active <- faults.in(env)
          _      <- ZIO.foreachDiscard(Color.values.toList)(color => observe(env, address, lb, color, if live.contains(color) then Set.empty else active, elb, cw))
        yield ()

  private def observe(env: EnvName, address: Address, lb: LoadBalancer, color: Color, faults: Set[Fault], elb: Elbv2Client, cw: CloudWatchClient)
      : IO[String, Unit] =
    elb.targetGroup(ServiceNaming.targetGroup(address, color)).mapError(_.message).flatMap:
      case None => ZIO.unit
      case Some(group) =>
        val dimensions = Map("LoadBalancer" -> ServiceProvider.arnSuffix(lb.arn, "loadbalancer/"), "TargetGroup" -> ServiceProvider.arnSuffix(group.arn, ""))
        for
          targets  <- elb.health(group.arn).mapError(_.message)
          answers  <- ZIO.foreach(targets)(t => status(t.id, t.port))
          errors    = answers.count(_._1 >= 500) + (if faults(Fault.Errors) then 5 else 0)
          seconds   = answers.map(_._2).sum / answers.size.max(1) + (if faults(Fault.Slow) then 2.0 else 0.0)
          _ <- (cw.putMetric("AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", errors.toDouble, dimensions) *>
                 cw.putMetric("AWS/ApplicationELB", "TargetResponseTime", seconds, dimensions)).mapError(_.message).when(targets.nonEmpty)
          _ <- cw.putMetric(Service.metricsNamespace(app), "UploadFailures", 5, Map("Service" -> service.value, "Color" -> color.toString))
                 .mapError(_.message).when(targets.nonEmpty && faults(Fault.FailingMetric))
        yield ()

  /** The status code and response time in seconds; 0 for no answer. */
  private def status(ip: String, port: Int): UIO[(Int, Double)] =
    ZIO.fromEither(URL.decode(s"http://$ip:$port/"))
      .flatMap(url => http.batched(Request.get(url)).map(_.status.code))
      .timeout(2.seconds)
      .map(_.getOrElse(0))
      .orElseSucceed(0)
      .timed
      .map((took, code) => (code, took.toMillis / 1000.0))
