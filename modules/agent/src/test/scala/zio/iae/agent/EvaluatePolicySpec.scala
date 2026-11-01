package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import java.time.Instant
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.engine.policy.*
import zio.iae.testkit.ScriptedModel
import zio.json.*
import zio.json.ast.{Json, JsonCursor}
import zio.test.*

object EvaluatePolicySpec extends ZIOSpecDefault:
  private val approvers = SlackGroup.from("infra-approvers").toOption.get
  private val owners    = SlackGroup.from("platform-owners").toOption.get
  private val clause    = "Any change that gives an app more IAM permissions needs one approver."

  private val policy = PolicyConfig(
    s"""# Approval policy
       |
       |Approvers are the @infra-approvers Slack group. Owners are @platform-owners.
       |
       |- $clause
       |- Otherwise: low and medium risk changes are fine.
       |""".stripMargin,
    owners,
    Set(owners, approvers),
    Set(EnvName("prod")),
  )

  private val at = Instant.parse("2026-11-01T10:00:00Z").nn

  private def fact(index: Int, risk: Risk, flags: Set[ChangeFlag] = Set.empty, when: Instant = at): ChangeFacts =
    val address = Address(AppName("photo-app"), EnvName("staging"), ResourceKind.Role, LogicalId("runtime"))
    ChangeFacts(ChangeId(s"$index:${address.show}"), EnvName("staging"), Subject.Resource(address, "Update", List("grants")), risk, flags, Origin.Human, Some("dev@example.com"), when)

  private val widening = fact(0, Risk.High, Set(ChangeFlag.IamWidening))

  private def verdicts(entries: String*): String = s"""{"verdicts":[${entries.mkString(",")}]}"""

  private def verdict(change: ChangeFacts, decision: String, approvals: Int, groups: List[String], clauses: List[String], confident: Boolean = true): String =
    s"""{"change":"${change.id.value}","decision":"$decision","approvals":$approvals,"groups":${groups.toJson},"clauses":${clauses.toJson},"reasoning":"because","confident":$confident}"""

  private val evaluator: ZIO[ScriptedModel & Client, Nothing, PolicyEvaluator] =
    for
      model <- ZIO.service[ScriptedModel]
      eval  <- Model.make(ConverseConfig(ApiKey("test-key"), model.url, ModelId("scripted"))).flatMap(EvaluatePolicy.make).orDieWith(e => RuntimeException(e.message))
    yield eval

  private def decide(answer: String, facts: List[ChangeFacts] = List(widening)): ZIO[ScriptedModel & Client, Nothing, PlanDecision] =
    for
      model    <- ZIO.service[ScriptedModel]
      _        <- model.replyText(answer)
      eval     <- evaluator
      verdicts <- eval.evaluate(policy, facts).orElseSucceed(Nil)
    yield Verdicts.decide(facts, verdicts, policy)

  def spec = suite("EvaluatePolicy")(
    test("the model's verdict decides the change, and the request carries the policy, the facts and an output schema"):
      for
        decision <- decide(verdicts(verdict(widening, "Approve", 1, List("infra-approvers"), List(clause))))
        requests <- ZIO.serviceWithZIO[ScriptedModel](_.requests)
        request   = requests.head.toJson
        schema    = requests.head.get(JsonCursor.field("outputConfig").isObject.field("textFormat").isObject.field("structure").isObject
                      .field("jsonSchema").isObject.field("schema").isString).map(_.value)
      yield assertTrue(
        decision.decision == Decision.Approve(1, Set(approvers)),
        requests.size == 1,
        request.contains("IAM permissions needs one approver"),
        request.contains("IamWidening"),
        schema.exists(_.contains("\"enum\":[\"Auto\",\"Approve\",\"Deny\"]")),
      ),
    test("Auto and Deny come through"):
      for
        auto <- decide(verdicts(verdict(widening, "Auto", 0, Nil, List("Otherwise: low and medium risk changes are fine."))))
        deny <- decide(verdicts(verdict(widening, "Deny", 0, Nil, List(clause))))
      yield assertTrue(auto.decision == Decision.Auto, deny.decision == Decision.Deny),
    test("output that doesn't decode means owners"):
      decide("I think this is fine").map(d => assertTrue(d.decision == Decision.Approve(1, Set(owners)))),
    test("a model error means owners"):
      for
        model    <- ZIO.service[ScriptedModel]
        _        <- model.reply(ScriptedModel.Turn.Failure(400, "bad request"))
        eval     <- evaluator
        result   <- eval.evaluate(policy, List(widening)).either
      yield assertTrue(result.isLeft),
    test("an unconfident verdict, a made-up clause or a missing change means owners"):
      val second = fact(1, Risk.Low)
      for
        unsure  <- decide(verdicts(verdict(widening, "Auto", 0, Nil, List(clause), confident = false)))
        madeUp  <- decide(verdicts(verdict(widening, "Auto", 0, Nil, List("Anything goes."))))
        missing <- decide(verdicts(verdict(widening, "Auto", 0, Nil, List(clause))), List(widening, second))
      yield assertTrue(
        unsure.decision == Decision.Approve(1, Set(owners)),
        madeUp.decision == Decision.Approve(1, Set(owners)),
        missing.changes.last.decision == Decision.Approve(1, Set(owners)),
      ),
    test("an unknown group becomes owners, and a group that isn't a handle makes the verdict doubtful"):
      for
        unknown <- decide(verdicts(verdict(widening, "Approve", 2, List("everyone-else"), List(clause))))
        invalid <- decide(verdicts(verdict(widening, "Approve", 1, List("Not A Group!"), List(clause))))
      yield assertTrue(
        unknown.decision == Decision.Approve(2, Set(owners)),
        invalid.decision == Decision.Approve(1, Set(owners)),
        invalid.changes.head.reasons.exists(_.contains("unclear")),
      ),
    test("the same facts on the same day are evaluated once"):
      for
        model <- ZIO.service[ScriptedModel]
        answer = verdicts(verdict(widening, "Approve", 1, List("infra-approvers"), List(clause)))
        _     <- model.replyText(answer, answer)
        eval  <- evaluator
        first <- eval.evaluate(policy, List(widening))
        later <- eval.evaluate(policy, List(widening.copy(at = at.plusSeconds(3600).nn)))
        once  <- model.requests.map(_.size)
        _     <- eval.evaluate(policy, List(widening.copy(at = at.plusSeconds(86400).nn)))
        twice <- model.requests.map(_.size)
      yield assertTrue(first == later, once == 1, twice == 2),
    test("no changes, no call"):
      for
        eval     <- evaluator
        result   <- eval.evaluate(policy, Nil)
        requests <- ZIO.serviceWithZIO[ScriptedModel](_.requests)
      yield assertTrue(result.isEmpty, requests.isEmpty),
  ).provide(ZLayer.scoped(ScriptedModel.make).orDie, Client.default.orDie) @@ TestAspect.sequential @@ TestAspect.withLiveClock
