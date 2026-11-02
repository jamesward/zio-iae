package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.engine.*
import zio.iae.testkit.ScriptedModel
import zio.test.*

/** The cost optimizer's report over scripted recommendations: posted once, with the model's review in the thread. */
object CostJobSpec extends ZIOSpecDefault:
  val app     = AppName("photo-app")
  val env     = EnvName("prod")
  val channel = "C_COSTS"

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def spec = suite("Cost optimizer")(
    test("the bill is posted once a week beside the projection, which is called out when it's off; each anomaly is posted once"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          ws       = repo.workspace(env)
          web      = Address(app, env, ResourceKind.Service, LogicalId("web"))
          found: Discovery = (_, _) => ZIO.succeed(List(Discovered(web, None, None)))
          priced: Pricing = new Pricing:
            def estimate(change: PlannedChange) = ZIO.none
            override def monthly(address: Address) = ZIO.some(40d)
          bills: Bills = new Bills:
            def spend(app: AppName, env: EnvName, from: java.time.LocalDate, to: java.time.LocalDate) =
              ZIO.some(Spend(from, to, 61.2, Map("Amazon Elastic Container Service" -> 58.7, "Amazon Simple Storage Service" -> 2.5)))
            def anomalies(env: EnvName, since: java.time.LocalDate) =
              ZIO.succeed(List(CostAnomaly("arn:aws:ce::1:anomalymonitor/m/a1", Some(java.time.LocalDate.parse("2026-10-30").nn), Some("AmazonECS"), 18.4, List("Amazon Elastic Container Service / us-east-1"))))
          runtime  = EnvRuntime(env, Engine.make(Drivers(Nil), found, AnyOrder).copy(pricing = priced, bills = bills), ws)
          slack   <- InMemorySlack.make(Map.empty)
          job      = CostJob(List(runtime), List(app), slack, slack.self, channel, None)
          _       <- job.sweep *> job.sweep *> job.anomalies *> job.anomalies
          posted  <- slack.posted(channel)
          bill     = posted.filter(_.text.startsWith("*Cost* `photo-app/prod`: $61.20"))
          spikes   = posted.filter(_.text.startsWith("*Cost anomaly*"))
        yield assertTrue(
          bill.size == 1,
          bill.head.text.contains("Amazon Elastic Container Service $58.70, Amazon Simple Storage Service $2.50."),
          bill.head.text.contains("iae projects $40.00 a month for its resources. The bill is 53% higher than that"),
          spikes.size == 1,
          spikes.head.text == "*Cost anomaly* in `prod`'s account: $18.40 more than expected on AmazonECS since 2026-10-30 (anomaly a1). Causes: Amazon Elastic Container Service / us-east-1.",
        ),
    test("right-sizing recommendations are reported once with their savings (the review needs a deployed commit; none here)"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          ws       = repo.workspace(env)
          web      = Address(app, env, ResourceKind.Service, LogicalId("web"))
          advice   = List(RightSizing(web, "Overprovisioned", 1024, 2048, 512, 1024, 35.79))
          none: Discovery = (_, _) => ZIO.succeed(Nil)
          recommend: Recommendations = (_, _) => ZIO.succeed(advice)
          runtime  = EnvRuntime(env, Engine.make(Drivers(Nil), none, AnyOrder).copy(recommendations = recommend), ws)
          slack   <- InMemorySlack.make(Map.empty)
          model   <- ScriptedModel.make.orDie
          bedrock <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          review  <- ReviewFindings.make(bedrock, "review-cost").mapError(_.message)
          _       <- model.reply(ScriptedModel.Turn.Text("Resize `web` to 512/1024 in `Main.scala`; it idles at 20% CPU."))
          job      = CostJob(List(runtime), List(app), slack, slack.self, channel, Some(review))
          _       <- job.sweep
          _       <- job.sweep
          posted  <- slack.posted(channel)
          // The review needs a deployed commit; with none, it is skipped and the report still stands.
        yield assertTrue(
          posted.size == 1,
          posted.head.text.startsWith("*Cost* `photo-app/prod`: right-sizing could save $35.79 a month"),
          posted.head.text.contains("Service `web` is overprovisioned: 1024 CPU / 2048 MiB -> 512 CPU / 1024 MiB, saves $35.79 a month"),
        ),
  ).provideShared(Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
