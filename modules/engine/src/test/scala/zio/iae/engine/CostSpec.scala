package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.{ComputeOptimizerFake, FlociLayer}
import zio.test.*

/** Plans carry each change's projected monthly cost; scaling a service shows what the extra tasks cost. */
object CostSpec extends ZIOSpecDefault:
  val app = AppName("cost-app")

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def web(port: Int, count: Int, cpu: Int = 256, memory: Int = 512): Manifest =
    val spec = ServiceSpec(5678, healthPath = "/", listenerPort = port, count = count, cpu = cpu, memory = memory, canary = CanarySpec(List(100), 1, alarmPeriodSeconds = 10), command = List("-text=hi"), image = Some("hashicorp/http-echo:1.0"))
    Manifest(app, List(ResourceDecl(LogicalId("web"), ResourceSpec.Service(spec))), Nil)

  def spec = suite("Costs on Floci")(
    test("Compute Optimizer's recommendation for a live service is priced as a monthly saving"):
      ZIO.scoped:
        for
          clients <- ZIO.environment[AwsEngine.Clients]
          http    <- ZIO.service[Client]
          aws     <- ZIO.service[AwsConfig]
          base     = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
          fake    <- ComputeOptimizerFake.make.orDie
          optimizer = ComputeOptimizerClient(AwsJsonClient(http, aws.copy(endpoint = Endpoint.parse(fake.url.encode).toOption)))
          watch    = base.watch.asInstanceOf[ServiceWatch]
          engine   = base.copy(recommendations = ComputeOptimizerRecommendations(optimizer, watch.provider, base.planner.discovery, Prices.fixed(PriceBook.usEast1)))
          env     <- EngineSpec.uniqueEnv
          port    <- Random.nextIntBounded(1000).map(14000 + _)
          _       <- engine.converge(web(port, 2, cpu = 1024, memory = 2048), env, None)
          none    <- engine.rightSizing(app, env)
          _       <- fake.recommend(s"/${env.value}-web-blue", "Overprovisioned", (1024, 2048), (512, 1024))
          found   <- engine.rightSizing(app, env)
          // Half a vCPU and 1 GB less on each of 2 tasks for a month.
          saving   = (0.5 * 0.04048 + 1 * 0.004445) * 2 * 730
        yield assertTrue(
          none.isEmpty,
          found.map(r => (r.finding, r.cpu, r.toCpu, r.memory, r.toMemory)) == List(("Overprovisioned", 1024, 512, 2048, 1024)),
          found.exists(r => math.abs(r.monthlySavings - saving) < 0.01),
        )
    ,
    test("a big bucket without Intelligent-Tiering is a storage finding, until its lifecycle tiers it"):
      for
        clients <- ZIO.environment[AwsEngine.Clients]
        engine   = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env     <- EngineSpec.uniqueEnv
        plain    = Manifest(app, List(ResourceDecl(LogicalId("photos"), ResourceSpec.Bucket(BucketSpec()))), Nil)
        _       <- engine.converge(plain, env, None)
        bucket   = BucketNaming(None).physical(Address(app, env, ResourceKind.Bucket, LogicalId("photos"))).toOption.get.value
        cw       = clients.get[DeployClients].cloudwatch
        // What S3 publishes daily for a bucket holding 200 GB in 1,000 objects.
        _       <- cw.putMetric("AWS/S3", "BucketSizeBytes", 200e9, Map("BucketName" -> bucket, "StorageType" -> "StandardStorage"))
        _       <- cw.putMetric("AWS/S3", "NumberOfObjects", 1000, Map("BucketName" -> bucket, "StorageType" -> "AllStorageTypes"))
        found   <- engine.storage(app, env)
        tiered   = Manifest(app, List(ResourceDecl(LogicalId("photos"), ResourceSpec.Bucket(BucketSpec(lifecycle = Some(Lifecycle(tieringAfterDays = Some(0))))))), Nil)
        _       <- engine.converge(tiered, env, None)
        after   <- engine.storage(app, env)
      yield assertTrue(
        found.map(f => (f.address.id.value, f.finding)) == List(("photos", "200.0 GB in S3 Standard and no Intelligent-Tiering")),
        // Half of 200 GB at the infrequent tier's price, less monitoring 1,000 objects.
        found.exists(f => math.abs(f.monthlySavings - (100 * (0.023 - 0.0125) - 0.0025)) < 0.01),
        after.isEmpty,
      )
    ,
    test("money is signed and rounded to cents"):
      assertTrue(PlanText.money(43.2) == "+$43.20", PlanText.money(-1.5) == "-$1.50", PlanText.money(0.001) == "$0.00")
    ,
    test("a new service's plan shows its cost; scaling it from 1 to 2 tasks costs one more task"):
      for
        clients <- ZIO.environment[AwsEngine.Clients]
        engine   = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env     <- EngineSpec.uniqueEnv
        port    <- Random.nextIntBounded(1000).map(15000 + _)
        created <- engine.plan(web(port, 1), env, None, Phase.Expand)
        _       <- engine.converge(web(port, 1), env, None)
        scaled  <- engine.plan(web(port, 2), env, None, Phase.Expand)
        now     <- engine.projected(app, env)
        task     = (256 / 1024d * 0.04048 + 512 / 1024d * 0.004445) * 730
        delta    = scaled.changes.head.cost.map(_.delta).getOrElse(0d)
      yield assertTrue(
        created.changes.head.cost.exists(c => c.before == 0 && c.after > task),
        PlanText.render(created).contains("cost: +$"),
        math.abs(delta - task) < 0.01,
        scaled.hash == scaled.copy(changes = scaled.changes.map(_.copy(cost = None))).hash,
        // What it costs now, as planned when it was created (the role costs nothing).
        now.exists(p => created.changes.head.cost.exists(c => math.abs(c.after - p) < 0.01)),
      ),
  ).provideShared(FlociLayer.live, Client.default, AwsEngine.clients) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
