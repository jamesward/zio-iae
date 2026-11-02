package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.PlanDecision
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** Applying a plan and recording it: the notice in the channel, the approval request's label and the audit evidence.
  * The agent does this itself, or the executor does after its own checks.
  */
object Applying:
  def run(
      slack: SlackApi,
      runtime: EnvRuntime,
      governance: Governance,
      author: Author,
      plan: Plan,
      decision: PlanDecision,
      commit: CommitSha,
      why: String,
      approval: Option[(SlackMessage, Set[String])],
      approvals: Approvals,
  ): IO[AgentError, ApplyReport] =
    val config = governance.config
    for
      report <- runtime.engine.apply(plan).mapError(AgentError.Engine(_))
      _ <- slack.post(config.slack.channel, s"Applied ${report.applied.size} change(s) to ${plan.app.value}/${plan.env.value} at `${commit.short}` ($why)\n```\n${PlanText.render(plan)}\n```", None, None)
      _      <- ZIO.foreachDiscard(approval)((m, _) => approvals.applied(m))
      now    <- Clock.instant
      change  = ChangeEvidence(
                  plan.app.value,
                  plan.env.value,
                  commit.value,
                  author.email,
                  author.slack,
                  governance.deployed.map(_.value),
                  plan.hash.value,
                  plan.phase.toString,
                  PlanText.render(plan).linesIterator.drop(1).map(_.trim).toList,
                  PlanText.describe(decision.decision),
                  decision.changes.flatMap(c => c.reasons.map(r => s"${c.change.value}: $r")),
                  approval.map((m, by) => ApprovalEvidence(config.slack.channel, m.ts, by.toList.sorted)),
                  report.applied.map(_.show),
                  now,
                )
      _ <- ZIO.foreachDiscard(runtime.evidence)(_.record(change).catchAll(e => ZIO.logError(s"evidence for ${plan.app.value}/${plan.env.value} plan ${plan.hash.short}: $e")))
    yield report

/** What the agent asks the executor to apply. The executor trusts none of it beyond identifying the plan: it
  * recomputes the plan, the floor and the approval itself.
  */
/** What the agent asks the executor to apply. `rollback` names the service of a rollback and `cleanup` the cutoff
  * date of a cleanup (orphans since then or earlier), which the executor plans from the env itself; `commit` is then
  * the commit deployed there.
  */
final case class ApplyRequest(
    env: String,
    app: String,
    commit: String,
    phase: String,
    plan: String,
    approval: Option[String],
    authorEmail: String,
    rollback: Option[String] = None,
    cleanup: Option[String] = None,
)
    derives Schema

object ApplyRequest:
  private val codec = JsonCodec.jsonCodec(Schema[ApplyRequest])

  def encode(r: ApplyRequest): String = codec.encoder.encodeJson(r, None).toString

  def decode(s: String): Either[String, ApplyRequest] = codec.decoder.decodeJson(s)

/** Where the agent sends apply requests. */
trait ApplyQueue:
  def submit(request: ApplyRequest): IO[AgentError, Unit]

final case class SqsApplyQueue(sqs: zio.iae.aws.client.SqsClient, url: zio.iae.aws.client.QueueUrl) extends ApplyQueue:
  def submit(request: ApplyRequest): IO[AgentError, Unit] =
    sqs.send(url, ApplyRequest.encode(request)).mapBoth(e => AgentError.Config(s"submitting to the executor: ${e.message}"), _ => ())
