package zio.iae.engine

import io.floci.testcontainers.FlociContainer
import zio.*
import zio.http.{Client, Request, URL}
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.{FlociLayer, FlociRegistry}
import zio.test.*

/** Services on Floci: created and deployed, then deployed again by canary, and a bad version rolled back. */
object ServiceDeploySpec extends ZIOSpecDefault:
  val app = AppName("svc-app")

  /** A bake that expects an alarm should outlast Floci's evaluation of it, which can take more than a 10s period. */
  def manifest(port: Int, command: List[String], image: String = "hashicorp/http-echo:1.0", alarms: List[AppAlarm] = Nil, bake: Int = 6): Manifest =
    val spec = ServiceSpec(5678, healthPath = "/", listenerPort = port, canary = CanarySpec(List(50, 100), bake, alarmPeriodSeconds = 10, alarms = alarms), command = command, image = Some(image))
    Manifest(app, List(ResourceDecl(LogicalId("web"), ResourceSpec.Service(spec))), Nil)

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def get(http: Client, ip: String, port: Int) =
    http.batched(Request.get(URL.decode(s"http://$ip:$port/").toOption.get)).flatMap(r => r.body.asString.map(b => (r.status.code, b.trim)))

  def ipOf(floci: FlociContainer): String =
    floci.getContainerInfo.nn.getNetworkSettings.nn.getNetworks.nn.values.nn.iterator.nn.next.nn.getIpAddress.nn

  def spec = suite("Services on Floci")(
    test("a service runs an image pushed to ECR, as a build pushes it"):
      for
        floci    <- ZIO.service[FlociContainer]
        http     <- ZIO.service[Client]
        aws      <- ZIO.service[AwsConfig]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(18000 + _)
        uri      <- EcrClient(AwsJsonClient(http, aws)).ensureRepository(s"iae/svc-app-${env.value}", Map.empty)
        _        <- FlociRegistry.push(aws, "000000000000", "hashicorp/http-echo:1.0", s"iae/svc-app-${env.value}", "c1")
        _        <- engine.converge(manifest(port, List("-text=from-ecr"), s"$uri:c1"), env, None)
        served   <- get(http, ipOf(floci), port).repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Int, String)](_._2 == "from-ecr") && Schedule.recurs(60)).map(_._2)
        watched  <- engine.alarms(app, env)
      yield assertTrue(
        served == (200, "from-ecr"),
        watched.exists(a => a.alarm.endsWith("-health") && !a.firing && a.gate == "health"),
        watched.exists(_.gate == "5xx"),
      ),
    test("cleanup prunes old task definitions but never the live or rollback ones, and keeps the images in use"):
      for
        http     <- ZIO.service[Client]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(15000 + _)
        address   = Address(app, env, ResourceKind.Service, LogicalId("web"))
        _        <- engine.converge(manifest(port, List("-text=v1")), env, None)
        ecs       = clients.get[DeployClients].ecs
        family    = ServiceNaming.family(address)
        container = ContainerSpec("app", "hashicorp/http-echo:1.0", 5678, Map.empty, 256, 512)
        // Three revisions newer than the live one that nothing runs, as abandoned deploys would leave.
        extra    <- ZIO.foreach(1 to 3)(_ => ecs.registerTaskDefinition(family, container, None, None))
        today    <- Clock.localDateTime.map(_.toLocalDate.nn)
        plan     <- engine.cleanup(app, env, today.minusDays(14).nn, keep = 1)
        _        <- engine.apply(plan)
        left     <- ecs.taskDefinitions(family)
        again    <- engine.cleanup(app, env, today.minusDays(14).nn, keep = 1)
        images   <- engine.artifacts(app, env)
      yield
        val pruned = plan.changes.collect { case PlannedChange(Change.Prune(_, versions), _, _, _) => versions }.flatten
        assertTrue(
          // The newest is kept (keep = 1), the live one is kept, the two in between go.
          pruned.toSet == extra.take(2).toSet,
          plan.changes.forall(_.risk == Risk.Low),
          PlanText.render(plan).contains("prune   Service web"),
          left.toSet.intersect(pruned.toSet).isEmpty,
          left.contains(extra.last),
          left.size == 2,
          again.isEmpty,
          images == Set("hashicorp/http-echo:1.0"),
        )
    ,
    test("a version whose own alarm goes off is rolled back, and the rollback names the alarm"):
      for
        floci    <- ZIO.service[FlociContainer]
        http     <- ZIO.service[Client]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(17000 + _)
        uploads   = List(AppAlarm("uploads", "UploadFailures", 3))
        _        <- engine.converge(manifest(port, List("-text=v1"), alarms = uploads), env, None)
        _        <- get(http, ipOf(floci), port).repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Int, String)](_._2 == "v1") && Schedule.recurs(60))
        cw        = clients.get[DeployClients].cloudwatch
        // What v2's containers publish while their uploads fail: v1 went to blue, so v2 is green.
        failures  = cw.putMetric("iae/svc-app", "UploadFailures", 5, Map("Service" -> "web", "Color" -> "green"))
        tripper  <- failures.repeat(Schedule.spaced(300.millis)).fork
        bad      <- engine.converge(manifest(port, List("-text=v2"), alarms = uploads, bake = 15), env, None).either
        _        <- tripper.interrupt
        after    <- get(http, ipOf(floci), port)
        names    <- cw.alarmNames(s"iae-svc-app-${env.value}-web-")
      yield assertTrue(
        bad.left.exists(_.message.contains("its uploads alarm went off at 50%")),
        after == (200, "v1"),
        names.toSet == Set(s"iae-svc-app-${env.value}-web-green-5xx", s"iae-svc-app-${env.value}-web-green-app-uploads",
          s"iae-svc-app-${env.value}-web-blue-5xx", s"iae-svc-app-${env.value}-web-blue-app-uploads"),
      ),
    test("a rollback puts the previous version back, marks the live one failed, and can't be repeated"):
      for
        floci    <- ZIO.service[FlociContainer]
        http     <- ZIO.service[Client]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(16000 + _)
        address   = Address(app, env, ResourceKind.Service, LogicalId("web"))
        serving   = (text: String) => get(http, ipOf(floci), port).repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Int, String)](_._2 == text) && Schedule.recurs(60)).map(_._2)
        _        <- engine.converge(manifest(port, List("-text=v1")), env, None)
        none     <- engine.rollback(address)
        _        <- serving("v1")
        v2        = manifest(port, List("-text=v2"))
        _        <- engine.converge(v2, env, None)
        _        <- serving("v2")
        plan     <- engine.rollback(address)
        _        <- engine.apply(plan)
        back     <- serving("v1")
        again    <- engine.converge(v2, env, None).either
        after    <- engine.rollback(address)
      yield assertTrue(
        none.isEmpty,
        plan.changes.map(_.change.productPrefix) == List("Rollback"),
        PlanText.render(plan).contains("rollback Service web"),
        back == (200, "v1"),
        again.left.exists(_.message.contains("push a fix")),
        after.isEmpty,
      ),
    test("a service is created and live, deployed again by canary, and a bad version is rolled back and not retried"):
      for
        floci    <- ZIO.service[FlociContainer]
        http     <- ZIO.service[Client]
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        port     <- Random.nextIntBounded(1000).map(19000 + _)
        serving   = (text: String) => get(http, ipOf(floci), port).repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Int, String)](_._2 == text) && Schedule.recurs(60)).map(_._2)
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
        v3        = manifest(port, List("-text=v3", "-status-code=500"), bake = 15)
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
