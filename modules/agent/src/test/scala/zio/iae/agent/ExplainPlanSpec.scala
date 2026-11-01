package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.testkit.ScriptedModel
import zio.json.*
import zio.test.*

object ExplainPlanSpec extends ZIOSpecDefault:
  val explainer: ZIO[ScriptedModel & Client, Nothing, PlanExplainer] =
    ZIO.serviceWithZIO[ScriptedModel](m => Model.make(ConverseConfig(ApiKey("k"), m.url, ModelId("scripted")))).flatMap(ExplainPlan.make).orDieWith(e => RuntimeException(e.message))

  val plan     = ApprovalsSpec.plan("uploads")
  val decision = zio.iae.engine.policy.PlanDecision(ApprovalsSpec.approve, Nil)

  def spec = suite("ExplainPlan")(
    test("the model gets the plan and the policy reasons as data, and its text comes back"):
      for
        model    <- ZIO.service[ScriptedModel]
        _        <- model.replyText("  This creates the uploads bucket in prod.  ")
        text     <- explainer.flatMap(_.explain(plan, decision))
        request  <- model.requests.map(_.head.toJson)
      yield assertTrue(
        text == "This creates the uploads bucket in prod.",
        request.contains("create  Bucket uploads"),
        request.contains("needs 1 approval(s) from @infra-approvers"),
        request.contains("Don't recommend approving or"),
      ),
    test("a model failure or an empty answer is an error, not an explanation"):
      for
        model <- ZIO.service[ScriptedModel]
        _     <- model.reply(ScriptedModel.Turn.Failure(503, "busy"), ScriptedModel.Turn.Failure(503, "busy"), ScriptedModel.Turn.Failure(503, "busy"))
        e     <- explainer
        down  <- e.explain(plan, decision).either
        _     <- model.replyText("   ")
        empty <- e.explain(plan, decision).either
      yield assertTrue(down.isLeft, empty.isLeft),
  ).provide(ZLayer.scoped(ScriptedModel.make).orDie, Client.default.orDie) @@ TestAspect.withLiveClock
