package zio.iae.agent.harness

import zio.*
import zio.http.{Client, Request, URL}
import zio.iae.*
import zio.iae.aws.{Color, ServiceNaming, ServiceProvider}
import zio.iae.aws.client.*

/** Steady requests to every task of the app's services, in every env, standing in for users. A real ALB publishes
  * each target group's 5xx count; Floci doesn't, so this publishes it the same way (`AWS/ApplicationELB`,
  * `HTTPCode_Target_5XX_Count` by load balancer and target group), and a canary's alarm goes off on real responses.
  */
final case class TrafficGenerator(http: Client, app: AppName, service: LogicalId, envs: List[(EnvName, AwsConfig)]):
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
        ZIO.foreachDiscard(Color.values.toList): color =>
          elb.targetGroup(ServiceNaming.targetGroup(address, color)).mapError(_.message).flatMap:
            case None => ZIO.unit
            case Some(group) =>
              for
                targets <- elb.health(group.arn).mapError(_.message)
                codes   <- ZIO.foreach(targets)(t => status(t.id, t.port))
                errors   = codes.count(_ >= 500)
                _ <- cw.putMetric(
                       "AWS/ApplicationELB",
                       "HTTPCode_Target_5XX_Count",
                       errors.toDouble,
                       Map("LoadBalancer" -> ServiceProvider.arnSuffix(lb.arn, "loadbalancer/"), "TargetGroup" -> ServiceProvider.arnSuffix(group.arn, "")),
                     ).mapError(_.message).when(targets.nonEmpty)
              yield ()

  private def status(ip: String, port: Int): UIO[Int] =
    ZIO.fromEither(URL.decode(s"http://$ip:$port/"))
      .flatMap(url => http.batched(Request.get(url)).map(_.status.code))
      .timeout(2.seconds)
      .map(_.getOrElse(0))
      .orElseSucceed(0)
