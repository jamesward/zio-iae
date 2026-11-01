package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*

enum ApprovalStatus derives CanEqual:
  case Approved(by: Set[String])
  case Rejected(by: String)
  case Pending(requestTs: String)

/** Approval requests live in Slack: a message from the agent's bot whose metadata says what it covers. Approval is
  * ✅ reactions from enough members of the required groups; ❌ from a member rejects. Nothing is stored elsewhere.
  */
final case class Approvals(slack: SlackApi, self: SlackIdentity, channel: String, expiry: Duration):
  import Approvals.*

  /** `extra` adds sections to a new request; it only runs when one is posted. */
  def status(
      plan: Plan,
      decision: PlanDecision,
      approve: Decision.Approve,
      author: Option[String],
      extra: IO[AgentError, List[String]] = ZIO.succeed(Nil),
  ): IO[AgentError, ApprovalStatus] =
    for
      now      <- Clock.instant
      messages <- slack.history(channel, now.minus(expiry).nn)
      wanted    = keys(plan)
      covering = messages.flatMap(m => m.request.map(m -> _)).filter: (m, r) =>
                   m.botId.contains(self.botId) && r.app == plan.app.value && r.env == plan.env.value &&
                   r.manifest == plan.manifest.value && wanted.subsetOf(r.changes.toSet) && r.expires > now.getEpochSecond
      status <- covering.maxByOption(_._1.ts) match
                  case None         => extra.flatMap(request(plan, decision, approve, author, now.plus(expiry).nn.getEpochSecond, _)).map(ApprovalStatus.Pending(_))
                  case Some((m, _)) => evaluate(m, approve, author)
    yield status

  private def evaluate(message: SlackMessage, approve: Decision.Approve, author: Option[String]): IO[AgentError, ApprovalStatus] =
    for
      members  <- ZIO.foreach(approve.groups.toList)(slack.groupMembers).map(_.flatten.toSet)
      eligible <- ZIO.filter(message.reactions.filter(r => r.name == approveReaction || r.name == rejectReaction).flatMap(_.users).distinct): user =>
                    if !members.contains(user) || author.contains(user) || user == self.userId then ZIO.succeed(false)
                    else slack.isBot(user).map(!_)
      approvers = message.reactions.filter(_.name == approveReaction).flatMap(_.users).filter(eligible.contains).toSet
      rejecter  = message.reactions.filter(_.name == rejectReaction).flatMap(_.users).find(eligible.contains)
    yield rejecter match
      case Some(user)                                  => ApprovalStatus.Rejected(user)
      case None if approvers.size >= approve.count     => ApprovalStatus.Approved(approvers)
      case None                                        => ApprovalStatus.Pending(message.ts)

  private def request(plan: Plan, decision: PlanDecision, approve: Decision.Approve, author: Option[String], expires: Long, extra: List[String])
      : IO[AgentError, String] =
    val text = (List(
      s"*Approval needed* for ${plan.app.value}/${plan.env.value}",
      s"```\n${PlanText.render(plan)}\n```",
      PlanText.render(decision),
    ) ++ extra ++ List(
      s"React :$approveReaction: to approve or :$rejectReaction: to reject. Expires <!date^$expires^{date_short_pretty} {time}|in ${expiry.toHours}h>.",
    )).mkString("\n")
    val metadata = ApprovalRequest(
      plan.app.value,
      plan.env.value,
      plan.manifest.value,
      plan.hash.value,
      keys(plan).toList.sorted,
      approve.count,
      approve.groups.toList.map(_.handle).sorted,
      author,
      expires,
    )
    slack.post(channel, text, None, Some(metadata))

object Approvals:
  val approveReaction = "white_check_mark"
  val rejectReaction  = "x"

  /** What a request covers: each change by kind and address. A plan recomputed after a partial apply is a subset. */
  def keys(plan: Plan): Set[String] = plan.changes.map(c => s"${c.change.productPrefix}:${c.address.show}").toSet
