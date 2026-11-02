package zio.iae.engine.policy

import java.time.Instant
import zio.iae.*
import zio.test.*

object VerdictsSpec extends ZIOSpecDefault:
  def group(handle: String): SlackGroup = SlackGroup.from(handle).toOption.get

  val owners    = group("platform-owners")
  val approvers = group("infra-approvers")
  val prod      = EnvName("prod")
  val staging   = EnvName("staging")
  val policy = PolicyConfig(
    text = "- Anything in dev can go ahead without asking.\n- In prod, deleting anything needs two owners.",
    owners = owners,
    groups = Set(owners, approvers),
    production = Set(prod),
  )

  def fact(id: String, env: EnvName = staging, risk: Risk = Risk.Low, subject: Subject = resource): ChangeFacts =
    ChangeFacts(ChangeId(id), env, subject, risk, Set.empty, Origin.Human, None, Instant.EPOCH)

  def resource: Subject = Subject.Resource(Address(AppName("photo-app"), EnvName("staging"), ResourceKind.Bucket, LogicalId("uploads")), "Create", Nil)

  def verdict(id: String, decision: Decision, clauses: List[String] = Nil, confident: Boolean = true) =
    ChangeVerdict(ChangeId(id), decision, clauses, "because", confident)

  val ownersOnce = Decision.Approve(1, Set(owners))

  def spec = suite("Verdicts")(
    test("a change adding more than its env's cost limit needs an owner, whatever the policy says"):
      val limited = policy.copy(costLimits = Map(prod -> 100d))
      val pricey  = fact("0", prod).copy(monthlyCost = Some(250))
      val cheap   = fact("1", prod).copy(monthlyCost = Some(40))
      val elsewhere = fact("2", staging).copy(monthlyCost = Some(250))
      assertTrue(
        Floor(pricey, limited) == (ownersOnce, List("it adds $250.00 a month, over prod's limit of $100.00")),
        Floor(cheap, limited)._1 == Decision.Auto,
        Floor(elsewhere, limited)._1 == Decision.Auto,
      )
    ,
    test("a plan whose changes together add more than the limit needs an owner, although each is under it"):
      val limited = policy.copy(costLimits = Map(prod -> 100d))
      val facts   = List(fact("0", prod).copy(monthlyCost = Some(60)), fact("1", prod).copy(monthlyCost = Some(70)))
      val decided = Verdicts.decide(facts, List(verdict("0", Decision.Auto), verdict("1", Decision.Auto)), limited)
      val cheaper = Verdicts.decide(facts.map(_.copy(monthlyCost = Some(30))), List(verdict("0", Decision.Auto), verdict("1", Decision.Auto)), limited)
      assertTrue(
        decided.decision == ownersOnce,
        decided.changes.exists(c => c.change.value == "plan" && c.reasons == List("together its changes add $130.00 a month, over prod's limit of $100.00")),
        cheaper.decision == Decision.Auto,
        Floor.plan(facts, policy).isEmpty,
      )
    ,
    test("the strictest decision wins and approvals combine"):
      assertTrue(
        Decision.strictest(Decision.Auto, Decision.Deny) == Decision.Deny,
        Decision.strictest(Decision.Approve(1, Set(approvers)), Decision.Approve(2, Set(owners))) == Decision.Approve(2, Set(approvers, owners)),
        Decision.strictest(Decision.Auto, ownersOnce) == ownersOnce,
      )
    ,
    test("a plan's decision is the strictest of its changes"):
      val result = Verdicts.decide(
        List(fact("a"), fact("b")),
        List(verdict("a", Decision.Auto), verdict("b", Decision.Approve(1, Set(approvers)))),
        policy,
      )
      assertTrue(result.decision == Decision.Approve(1, Set(approvers)))
    ,
    test("missing, duplicate, unconfident and made-up verdicts fall back to the owners"):
      val facts = List(fact("missing"), fact("twice"), fact("unsure"), fact("invented"))
      val verdicts = List(
        verdict("twice", Decision.Auto),
        verdict("twice", Decision.Auto),
        verdict("unsure", Decision.Auto, confident = false),
        verdict("invented", Decision.Auto, clauses = List("- Everything is fine in prod.")),
      )
      val result = Verdicts.decide(facts, verdicts, policy)
      assertTrue(result.changes.forall(_.decision == ownersOnce))
    ,
    test("a quoted clause that is in the policy is accepted"):
      val result = Verdicts.decide(List(fact("a")), List(verdict("a", Decision.Auto, clauses = List("- Anything in dev can go ahead without asking."))), policy)
      assertTrue(result.decision == Decision.Auto)
    ,
    test("unknown groups are replaced by the owners"):
      val result = Verdicts.decide(List(fact("a")), List(verdict("a", Decision.Approve(1, Set(group("random-people"))))), policy)
      assertTrue(result.decision == ownersOnce)
    ,
    test("the floor can't be overridden"):
      val result = Verdicts.decide(
        List(fact("policy", subject = Subject.PolicyText), fact("self", subject = Subject.AgentDeployment), fact("critical", env = prod, risk = Risk.Critical), fact("critical-staging", risk = Risk.Critical)),
        List(verdict("policy", Decision.Auto), verdict("self", Decision.Auto), verdict("critical", Decision.Auto), verdict("critical-staging", Decision.Auto)),
        policy,
      )
      assertTrue(result.changes.map(_.decision) == List(ownersOnce, ownersOnce, ownersOnce, Decision.Auto))
    ,
    test("deny always wins"):
      val result = Verdicts.decide(List(fact("a"), fact("b")), List(verdict("a", Decision.Auto), verdict("b", Decision.Deny)), policy)
      assertTrue(result.decision == Decision.Deny)
    ,
    test("the default risk table"):
      val table = RiskTable(approvers, owners, production = Set(prod), free = Set(EnvName("dev")))
      val verdicts = table.evaluate(
        List(fact("dev", env = EnvName("dev"), risk = Risk.Critical), fact("stg", risk = Risk.Medium), fact("stg-high", risk = Risk.High), fact("prod-med", env = prod, risk = Risk.Medium), fact("prod-crit", env = prod, risk = Risk.Critical))
      )
      assertTrue(
        verdicts.map(_.decision) == List(
          Decision.Auto,
          Decision.Auto,
          Decision.Approve(1, Set(approvers)),
          Decision.Approve(1, Set(approvers)),
          Decision.Approve(2, Set(owners)),
        )
      )
    ,
  )
