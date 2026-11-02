package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** Plans carry each change's projected monthly cost; scaling a service shows what the extra tasks cost. */
object CostSpec extends ZIOSpecDefault:
  val app = AppName("cost-app")

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def web(port: Int, count: Int): Manifest =
    val spec = ServiceSpec(5678, healthPath = "/", listenerPort = port, count = count, canary = CanarySpec(List(100), 1, alarmPeriodSeconds = 10), command = List("-text=hi"), image = Some("hashicorp/http-echo:1.0"))
    Manifest(app, List(ResourceDecl(LogicalId("web"), ResourceSpec.Service(spec))), Nil)

  def spec = suite("Costs on Floci")(
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
        task     = (256 / 1024d * 0.04048 + 512 / 1024d * 0.004445) * 730
        delta    = scaled.changes.head.cost.map(_.delta).getOrElse(0d)
      yield assertTrue(
        created.changes.head.cost.exists(c => c.before == 0 && c.after > task),
        PlanText.render(created).contains("cost: +$"),
        math.abs(delta - task) < 0.01,
        scaled.hash == scaled.copy(changes = scaled.changes.map(_.copy(cost = None))).hash,
      ),
  ).provideShared(FlociLayer.live, Client.default, AwsEngine.clients) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
