package zio.iae.engine

import io.floci.testcontainers.FlociContainer
import zio.*
import zio.http.{Client, Request, URL}
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** Services on Floci: created and deployed, then deployed again by canary, and a bad version rolled back. */
object ServiceDeploySpec extends ZIOSpecDefault:
  val app = AppName("svc-app")

  def manifest(port: Int, command: List[String]): Manifest =
    val spec = ServiceSpec(5678, healthPath = "/", listenerPort = port, canary = CanarySpec(List(50, 100), 6, alarmPeriodSeconds = 10), command = command, image = Some("hashicorp/http-echo:1.0"))
    Manifest(app, List(ResourceDecl(LogicalId("web"), ResourceSpec.Service(spec))), Nil)

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def spec = suite("Services on Floci")(
    test("a service is created and live, deployed again by canary, and a bad version is rolled back and not retried"):
      for
        floci    <- ZIO.service[FlociContainer]
        http     <- ZIO.service[Client]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(19000 + _)
        ip        = floci.getContainerInfo.nn.getNetworkSettings.nn.getNetworks.nn.values.nn.iterator.nn.next.nn.getIpAddress.nn
        get       = http.batched(Request.get(URL.decode(s"http://$ip:$port/").toOption.get)).flatMap(r => r.body.asString.map(b => (r.status.code, b.trim)))
        serving   = (text: String) => get.repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Int, String)](_._2 == text) && Schedule.recurs(60)).map(_._2)
        v1        = manifest(port, List("-text=v1"))
        _        <- engine.converge(v1, env, None)
        first    <- serving("v1")
        v2        = manifest(port, List("-text=v2"))
        planned  <- engine.plan(v2, env, None, Phase.Expand)
        _        <- engine.converge(v2, env, None)
        second   <- serving("v2")
        address   = Address(app, env, ResourceKind.Service, LogicalId("web"))
        elb       = clients.get[DeployClients].elb
        cw        = clients.get[DeployClients].cloudwatch
        tags1    <- elb.loadBalancer(ServiceNaming.loadBalancer(address)).someOrFail("no lb").flatMap(lb => elb.tags(lb.arn))
        // The bad version: its alarm goes off once it has traffic.
        v3        = manifest(port, List("-text=v3", "-status-code=500"))
        lb       <- elb.loadBalancer(ServiceNaming.loadBalancer(address)).someOrFail("no lb")
        blue     <- elb.targetGroup(ServiceNaming.targetGroup(address, Color.Blue)).someOrFail("no blue")
        // What a real ALB publishes for the new color while it answers 500s.
        errors    = cw.putMetric("AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", 5, Map("LoadBalancer" -> ServiceProvider.arnSuffix(lb.arn, "loadbalancer/"), "TargetGroup" -> ServiceProvider.arnSuffix(blue.arn, "")))
        tripper  <- errors.repeat(Schedule.spaced(300.millis)).fork
        bad      <- engine.converge(v3, env, None).either
        _        <- tripper.interrupt
        after    <- serving("v2")
        again    <- engine.converge(v3, env, None).either
        tags2    <- elb.loadBalancer(ServiceNaming.loadBalancer(address)).someOrFail("no lb").flatMap(lb => elb.tags(lb.arn))
      yield assertTrue(
        first == (200, "v1"),
        planned.changes.map(_.change.productPrefix) == List("Update"),
        second == (200, "v2"),
        tags1.get(ServiceProvider.ColorTag).contains("green"),
        bad.left.exists(_.message.contains("rolled back")),
        after == (200, "v2"),
        again.left.exists(_.message.contains("push a fix")),
        tags2.get(ServiceProvider.ColorTag).contains("green"),
      )
  ).provideShared(FlociLayer.container, FlociLayer.awsConfig, Client.default, AwsEngine.clients) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(8.minutes)
