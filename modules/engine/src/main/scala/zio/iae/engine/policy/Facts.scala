package zio.iae.engine.policy

import java.time.Instant
import zio.iae.*
import zio.iae.engine.Plan
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum

/** Who or what a change comes from. Policies can treat the agent's own changes differently from people's. */
@simpleEnum
enum Origin derives Schema, CanEqual:
  case Human, Agent, Alert, Drift, Cleanup, Cost, Security

opaque type SlackGroup = String

object SlackGroup:
  private val pattern = "[a-z0-9][a-z0-9._-]{0,79}".r

  def from(handle: String): Either[String, SlackGroup] =
    val bare = handle.stripPrefix("@")
    Either.cond(pattern.matches(bare), bare, s"'$handle' is not a Slack user group handle")

  extension (group: SlackGroup) def handle: String = group

  given CanEqual[SlackGroup, SlackGroup] = CanEqual.derived
  given Ordering[SlackGroup]             = Ordering.String
  given Schema[SlackGroup]               = Schema[String].transformOrFail(from, g => Right(g))

/** What is being changed. Policy and the agent's own deployment are subjects too, so the floor can gate them. */
enum Subject derives Schema, CanEqual:
  case Resource(address: Address, action: String, fields: List[String])
  case PolicyText
  case AgentDeployment

opaque type ChangeId = String

object ChangeId:
  def apply(value: String): ChangeId = value

  extension (id: ChangeId) def value: String = id

  given CanEqual[ChangeId, ChangeId] = CanEqual.derived
  given Schema[ChangeId]             = Schema[String].transform(apply, _.value)

/** The structured description of one change that policy evaluation sees. Deliberately no free text from commits,
  * PRs or Slack: those can't argue for a weaker verdict.
  */
final case class ChangeFacts(
    id: ChangeId,
    env: EnvName,
    subject: Subject,
    risk: Risk,
    flags: Set[ChangeFlag],
    origin: Origin,
    author: Option[String],
    at: Instant,
    /** The projected change in monthly cost, in USD, when the change has an estimate. */
    monthlyCost: Option[Double] = None,
) derives Schema, CanEqual

object ChangeFacts:
  def of(plan: Plan, origin: Origin, author: Option[String], at: Instant): List[ChangeFacts] =
    plan.changes.zipWithIndex.map: (planned, index) =>
      val (action, fields) = planned.change match
        case Change.Create(_, _)         => ("Create", Nil)
        case Change.Update(_, _, fields) => ("Update", fields.toList.map(_.field))
        case Change.Orphan(_, _)         => ("Orphan", Nil)
        case Change.Delete(_)            => ("Delete", Nil)
        case Change.Rollback(_, _, _)    => ("Rollback", Nil)
      ChangeFacts(
        ChangeId(s"$index:${planned.address.show}"),
        plan.env,
        Subject.Resource(planned.address, action, fields),
        planned.risk,
        planned.flags,
        // A revert of a hand change is the drift job's, whatever started the reconcile.
        if planned.flags.contains(ChangeFlag.Drift) && origin == Origin.Human then Origin.Drift else origin,
        author,
        at,
        planned.cost.map(c => math.round(c.delta * 100) / 100d),
      )
