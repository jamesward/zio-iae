package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*

enum ApprovalStatus derives CanEqual:
  case Approved(by: Set[String], request: SlackMessage)
  case Rejected(by: String, request: SlackMessage)
  case Pending(requestTs: String)

/** Approval requests live in Slack: a message from the agent's bot whose metadata says what it covers. Approval is
  * ✅ reactions from enough members of the required groups; ❌ from a member rejects. Nothing is stored elsewhere.
  *
  * A request's first line says where it stands: `*Approval needed*` while open, then `*Approved*` once applied,
  * `*Rejected*`, `*Superseded*` when a newer request for the env replaces it, or `*Expired*`. Superseded, expired
  * and approved requests no longer cover anything. A rejected one keeps covering its plan, so the change stays
  * stopped until a new commit.
  */
final case class Approvals(slack: SlackApi, self: SlackIdentity, channel: String, expiry: Duration, reminder: Duration = 1.hour):
  import Approvals.*

  /** `extra` adds sections to a new request and `posted` runs after one is posted, with its ts. */
  def status(
      plan: Plan,
      decision: PlanDecision,
      approve: Decision.Approve,
      author: Option[String],
      extra: IO[AgentError, List[String]] = ZIO.succeed(Nil),
      posted: String => UIO[Unit] = _ => ZIO.unit,
  ): IO[AgentError, ApprovalStatus] =
    for
      now      <- Clock.instant
      messages <- slack.history(channel, now.minus(expiry.multipliedBy(2)).nn)
      ours = messages.flatMap(m => m.request.map(m -> _)).filter: (m, r) =>
               m.botId.contains(self.botId) && r.app == plan.app.value && r.env == plan.env.value && !closed(m.text)
      (expired, live) = ours.partition(_._2.expires <= now.getEpochSecond)
      _       <- ZIO.foreachDiscard(expired)((m, r) => label(m, r, Expired))
      wanted   = keys(plan)
      covering = live.filter((_, r) => r.manifest == plan.manifest.value && wanted.subsetOf(r.changes.toSet))
      status <- covering.maxByOption(_._1.ts) match
                  case None =>
                    for
                      ts <- extra.flatMap(request(plan, decision, approve, author, now.plus(expiry).nn.getEpochSecond, _))
                      _  <- ZIO.foreachDiscard(live)((m, r) => label(m, r, Superseded))
                      _  <- posted(ts)
                    yield ApprovalStatus.Pending(ts)
                  case Some((m, r)) =>
                    evaluate(m, approve, author).tap:
                      case ApprovalStatus.Pending(_)     => remind(m, r, approve, now)
                      case ApprovalStatus.Rejected(_, _) => label(m, r, Rejected).unless(m.text.startsWith(Rejected))
                      case ApprovalStatus.Approved(_, _) => ZIO.unit
    yield status

  /** After an approved plan is applied, its request says so and covers nothing more. */
  def applied(request: SlackMessage): IO[AgentError, Unit] =
    ZIO.foreachDiscard(request.request)(label(request, _, Approved))

  private def evaluate(message: SlackMessage, approve: Decision.Approve, author: Option[String]): IO[AgentError, ApprovalStatus] =
    for
      members  <- ZIO.foreach(approve.groups.toList)(slack.groupMembers).map(_.flatten.toSet)
      eligible <- ZIO.filter(message.reactions.filter(r => r.name == approveReaction || r.name == rejectReaction).flatMap(_.users).distinct): user =>
                    if !members.contains(user) || author.contains(user) || user == self.userId then ZIO.succeed(false)
                    else slack.isBot(user).map(!_)
      approvers = message.reactions.filter(_.name == approveReaction).flatMap(_.users).filter(eligible.contains).toSet
      rejecter  = message.reactions.filter(_.name == rejectReaction).flatMap(_.users).find(eligible.contains)
    yield rejecter match
      case Some(user)                                  => ApprovalStatus.Rejected(user, message)
      case None if approvers.size >= approve.count     => ApprovalStatus.Approved(approvers, message)
      case None                                        => ApprovalStatus.Pending(message.ts)

  /** Once per request, after the reminder period: a thread reply that mentions the groups that can approve. */
  private def remind(message: SlackMessage, request: ApprovalRequest, approve: Decision.Approve, now: java.time.Instant): IO[AgentError, Unit] =
    val due = !message.postedAt.plus(reminder).nn.isAfter(now) && !message.text.contains(reminded)
    ZIO.when(due):
      for
        mentions <- ZIO.foreach(approve.groups.toList.sorted)(g => slack.groupId(g).map(_.fold(s"@${g.handle}")(id => s"<!subteam^$id>")))
        _ <- slack.post(channel, s"${mentions.mkString(" ")} this still needs ${approve.count} approval(s); it expires <!date^${request.expires}^{date_short_pretty} {time}|soon>.", Some(message.ts), None)
        _ <- slack.update(channel, message.ts, s"${message.text}\n$reminded", Some(request))
      yield ()
    .unit

  private def label(message: SlackMessage, request: ApprovalRequest, status: String): IO[AgentError, Unit] =
    val text = if message.text.startsWith("*") then status + message.text.drop(message.text.indexOf('*', 1) + 1) else s"$status ${message.text}"
    slack.update(channel, message.ts, text, Some(request))

  private def request(plan: Plan, decision: PlanDecision, approve: Decision.Approve, author: Option[String], expires: Long, extra: List[String])
      : IO[AgentError, String] =
    val text = (List(
      s"$Open for ${plan.app.value}/${plan.env.value}",
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

  val Open       = "*Approval needed*"
  val Approved   = "*Approved*"
  val Rejected   = "*Rejected*"
  val Superseded = "*Superseded*"
  val Expired    = "*Expired*"
  val reminded   = ":bell: reminded"

  /** Requests that no longer cover anything. */
  def closed(text: String): Boolean = List(Approved, Superseded, Expired).exists(text.startsWith)

  /** What a request covers: each change by kind and address. A plan recomputed after a partial apply is a subset. */
  def keys(plan: Plan): Set[String] = plan.changes.map(c => s"${c.change.productPrefix}:${c.address.show}").toSet
