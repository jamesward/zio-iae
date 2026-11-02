package zio.iae.engine.policy

import zio.*
import zio.iae.*
import zio.schema.{Schema, derived}

enum Decision derives Schema, CanEqual:
  case Auto
  case Approve(count: Int, groups: Set[SlackGroup])
  case Deny

object Decision:
  /** Deny beats Approve beats Auto; two approvals combine into the larger count from each of the groups of both, which
    * is at least as strict as either.
    */
  def strictest(a: Decision, b: Decision): Decision = (a, b) match
    case (Deny, _) | (_, Deny)                  => Deny
    case (Approve(c1, g1), Approve(c2, g2))     => Approve(c1.max(c2), g1 ++ g2)
    case (approve: Approve, Auto)               => approve
    case (Auto, approve: Approve)               => approve
    case (Auto, Auto)                           => Auto

/** One change's verdict as the policy evaluator returns it (the natural-language evaluator in the agent, or a table). */
final case class ChangeVerdict(change: ChangeId, decision: Decision, clauses: List[String], reasoning: String, confident: Boolean)
    derives Schema, CanEqual

/** The policy's settings that code relies on: the owners group, the groups verdicts may name, and which envs are
  * production for the built-in floor.
  */
/** `costLimits`: per env, the monthly cost (USD) a change, or a whole plan, may add before it needs an owner. */
final case class PolicyConfig(text: String, owners: SlackGroup, groups: Set[SlackGroup], production: Set[EnvName], costLimits: Map[EnvName, Double] = Map.empty)

final case class ChangeDecision(change: ChangeId, decision: Decision, reasons: List[String]) derives Schema, CanEqual

final case class PlanDecision(decision: Decision, changes: List[ChangeDecision]) derives Schema, CanEqual

object Verdicts:
  /** Combines the evaluator's verdicts with the fail-safe rules and the built-in floor. Anything missing or doubtful
    * becomes an approval by the owners; the floor can only make a decision stricter.
    */
  def decide(facts: List[ChangeFacts], verdicts: List[ChangeVerdict], policy: PolicyConfig): PlanDecision =
    val changes = facts.map: fact =>
      val (evaluated, why) = failSafe(fact, verdicts.filter(_.change == fact.id), policy)
      val (floor, floorWhy) = Floor(fact, policy)
      ChangeDecision(fact.id, Decision.strictest(evaluated, floor), why ++ floorWhy)
    val all = changes ++ Floor.plan(facts, policy).toList
    PlanDecision(all.map(_.decision).foldLeft[Decision](Decision.Auto)(Decision.strictest), all)

  private def failSafe(fact: ChangeFacts, verdicts: List[ChangeVerdict], policy: PolicyConfig): (Decision, List[String]) =
    val owners = Decision.Approve(1, Set(policy.owners))
    verdicts match
      case Nil                          => (owners, List("the policy gave no verdict for this change"))
      case _ :: _ :: _                  => (owners, List("the policy gave more than one verdict for this change"))
      case List(v) if !v.confident      => (owners, List(s"the policy was unclear: ${v.reasoning}"))
      case List(v) if v.clauses.exists(c => !policy.text.contains(c)) =>
        (owners, List("the verdict quoted text that isn't in the policy"))
      case List(v) =>
        v.decision match
          case Decision.Approve(count, groups) =>
            val unknown = groups.filterNot(policy.groups.contains)
            val known   = groups.filter(policy.groups.contains) ++ Option.when(unknown.nonEmpty)(policy.owners)
            val note    = Option.when(unknown.nonEmpty)(s"unknown groups ${unknown.map(_.handle).mkString(", ")} replaced by owners")
            (Decision.Approve(count.max(1), known), v.reasoning :: note.toList)
          case other => (other, List(v.reasoning))

/** Rules the policy text can't override. */
object Floor:
  def apply(fact: ChangeFacts, policy: PolicyConfig): (Decision, List[String]) =
    val owners = Decision.Approve(1, Set(policy.owners))
    fact.subject match
      case Subject.PolicyText      => (owners, List("changes to the policy always need an owner"))
      case Subject.AgentDeployment => (owners, List("changes to the agent's own deployment always need an owner"))
      case Subject.Resource(_, _, _) if fact.risk == Risk.Critical && policy.production.contains(fact.env) =>
        (owners, List("critical changes in production always need an owner"))
      case Subject.Resource(_, _, _) if fact.monthlyCost.exists(added => policy.costLimits.get(fact.env).exists(added > _)) =>
        val limit = policy.costLimits(fact.env)
        (owners, List(f"it adds $$${fact.monthlyCost.get}%.2f a month, over ${fact.env.value}'s limit of $$$limit%.2f"))
      case Subject.Resource(_, _, _) => (Decision.Auto, Nil)

  /** The plan's own floor: its changes together adding more than the env's cost limit need an owner, although each
    * may be under it. None when they don't, or the env has no limit.
    */
  def plan(facts: List[ChangeFacts], policy: PolicyConfig): Option[ChangeDecision] =
    val priced = facts.filter(f => f.subject.isInstanceOf[Subject.Resource] && f.monthlyCost.isDefined)
    for
      env   <- priced.headOption.map(_.env)
      limit <- policy.costLimits.get(env)
      total  = math.round(priced.flatMap(_.monthlyCost).sum * 100) / 100d
      if priced.size > 1 && total > limit
    yield ChangeDecision(ChangeId("plan"), Decision.Approve(1, Set(policy.owners)), List(f"together its changes add $$$total%.2f a month, over ${env.value}'s limit of $$$limit%.2f"))

/** A policy without an LLM: the default risk table from the spec. Used by the CLI and in tests. */
final case class RiskTable(approvers: SlackGroup, owners: SlackGroup, production: Set[EnvName], free: Set[EnvName]):
  def evaluate(facts: List[ChangeFacts]): List[ChangeVerdict] =
    facts.map: fact =>
      val decision =
        if free.contains(fact.env) then Decision.Auto
        else if production.contains(fact.env) then
          fact.risk match
            case Risk.Low                => Decision.Auto
            case Risk.Medium | Risk.High => Decision.Approve(1, Set(approvers))
            case Risk.Critical           => Decision.Approve(2, Set(owners))
        else
          fact.risk match
            case Risk.Low | Risk.Medium   => Decision.Auto
            case Risk.High | Risk.Critical => Decision.Approve(1, Set(approvers))
      ChangeVerdict(fact.id, decision, Nil, s"default risk table: ${fact.risk} in ${fact.env.value}", confident = true)

/** Turns a policy and change facts into verdicts: the natural-language evaluator in the agent, or the risk table. A
  * failure is treated like a missing verdict by `Verdicts.decide`, so it ends in an owner approval, not an apply.
  */
trait PolicyEvaluator:
  def evaluate(policy: PolicyConfig, facts: List[ChangeFacts]): IO[String, List[ChangeVerdict]]

final case class RiskTableEvaluator(approvers: SlackGroup) extends PolicyEvaluator:
  def evaluate(policy: PolicyConfig, facts: List[ChangeFacts]): IO[String, List[ChangeVerdict]] =
    ZIO.succeed(RiskTable(approvers, policy.owners, policy.production, free = Set.empty).evaluate(facts))
