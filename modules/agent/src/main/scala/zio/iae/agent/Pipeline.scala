package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*

/** One env's view of the world: its engine (its account) and its own clone of the repo. */
final case class EnvRuntime(env: EnvName, engine: Engine, workspace: GitWorkspace)

enum Outcome derives CanEqual:
  /** Nothing left to do at this commit. */
  case Converged(commit: CommitSha)
  case WaitingForPreviousEnv(previous: EnvName)
  case WaitingForApproval(app: AppName, requestTs: String)
  case Stopped(app: AppName, reason: String)

/** Reconciles one env to the head of the branch: expand, then contract, each through the user's policy. Level-
  * triggered and idempotent: running it again after a crash or a dropped event just does what is left.
  */
final case class Pipeline(manifests: ManifestSource, evaluator: PolicyEvaluator, slack: SlackApi, self: SlackIdentity, approvalExpiry: Duration):
  def reconcile(envs: List[EnvRuntime], index: Int): IO[AgentError, Outcome] =
    val runtime = envs(index)
    for
      commit           <- runtime.workspace.sync
      (config, text)   <- RepoConfig.load(runtime.workspace.dir)
      authorEmail      <- runtime.workspace.author
      author            = config.identities.get(authorEmail)
      apps             <- manifests.manifests(runtime.workspace.dir, config)
      gate             <- ZIO.foreach(Option.when(index > 0)(envs(index - 1)))(previous => converged(previous, apps, commit).map(previous -> _))
      outcome <- gate match
                   case Some((previous, false)) => ZIO.succeed(Outcome.WaitingForPreviousEnv(previous.env))
                   case _                       => applyAll(runtime, config, text, author, apps, commit)
      _ <- ZIO.logInfo(s"reconciled ${runtime.env.value} at ${commit.short}: $outcome")
    yield outcome

  /** The previous env is done with this commit when neither phase has anything left to do there. */
  private def converged(previous: EnvRuntime, apps: List[Manifest], commit: CommitSha): IO[AgentError, Boolean] =
    ZIO
      .forall(apps)(m => ZIO.forall(List(Phase.Expand, Phase.Contract))(p => previous.engine.plan(m, previous.env, Some(commit), p).map(_.isEmpty)))
      .mapError(AgentError.Engine(_))

  private def applyAll(runtime: EnvRuntime, config: RepoConfig, text: String, author: Option[String], apps: List[Manifest], commit: CommitSha): IO[AgentError, Outcome] =
    val approvals = Approvals(slack, self, config.slack.channel, approvalExpiry)
    val steps     = for app <- apps; phase <- List(Phase.Expand, Phase.Contract) yield (app, phase)
    ZIO.foldLeft(steps)(Outcome.Converged(commit)): (outcome, step) =>
      outcome match
        case Outcome.Converged(_) => phase(runtime, config, text, author, approvals, step._1, step._2, commit)
        case blocked              => ZIO.succeed(blocked)

  private def phase(
      runtime: EnvRuntime,
      config: RepoConfig,
      text: String,
      author: Option[String],
      approvals: Approvals,
      manifest: Manifest,
      phase: Phase,
      commit: CommitSha,
  ): IO[AgentError, Outcome] =
    for
      plan     <- runtime.engine.plan(manifest, runtime.env, Some(commit), phase).mapError(AgentError.Engine(_))
      decision <- decide(config.policy(text), plan, author)
      outcome <- (plan.isEmpty, decision.decision) match
                   case (true, _)                => ZIO.succeed(Outcome.Converged(commit))
                   case (_, Decision.Auto)       => applied(runtime, config, plan, "the policy lets it go ahead", commit)
                   case (_, Decision.Deny)       => stopped(config, plan, decision, "the policy denies it")
                   case (_, approve: Decision.Approve) =>
                     approvals.status(plan, decision, approve, author).flatMap:
                       case ApprovalStatus.Approved(by)  => applied(runtime, config, plan, s"approved by ${by.toList.sorted.map(u => s"<@$u>").mkString(", ")}", commit)
                       case ApprovalStatus.Rejected(by)  => stopped(config, plan, decision, s"<@$by> rejected it")
                       case ApprovalStatus.Pending(ts)   => ZIO.succeed(Outcome.WaitingForApproval(plan.app, ts))
    yield outcome

  private def decide(policy: PolicyConfig, plan: Plan, author: Option[String]): UIO[PlanDecision] =
    for
      now      <- Clock.instant
      facts     = ChangeFacts.of(plan, Origin.Human, author, now)
      verdicts <- evaluator.evaluate(policy, facts).catchAll(e => ZIO.logWarning(s"policy evaluation failed, asking the owners: $e").as(Nil))
    yield Verdicts.decide(facts, verdicts, policy)

  private def applied(runtime: EnvRuntime, config: RepoConfig, plan: Plan, why: String, commit: CommitSha): IO[AgentError, Outcome] =
    for
      report <- runtime.engine.apply(plan).mapError(AgentError.Engine(_))
      _      <- notice(config, s"Applied ${report.applied.size} change(s) to ${plan.app.value}/${plan.env.value} at `${commit.short}` ($why)\n```\n${PlanText.render(plan)}\n```")
    yield Outcome.Converged(commit)

  /** Stopping is announced once per plan, not on every reconcile. */
  private def stopped(config: RepoConfig, plan: Plan, decision: PlanDecision, why: String): IO[AgentError, Outcome] =
    val marker = s"plan ${plan.hash.short}"
    for
      now    <- Clock.instant
      recent <- slack.history(config.slack.channel, now.minus(approvalExpiry).nn)
      _ <- notice(config, s"Not applying ${plan.app.value}/${plan.env.value} ($marker): $why\n${PlanText.render(decision)}")
             .unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker) && m.text.startsWith("Not applying")))
    yield Outcome.Stopped(plan.app, why)

  private def notice(config: RepoConfig, text: String): IO[AgentError, Unit] =
    slack.post(config.slack.channel, text, None, None).unit
