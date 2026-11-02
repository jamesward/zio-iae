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
