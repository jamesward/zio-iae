package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*

/** One env's view of the world: its engine (its account) and its own clone of the repo. */
final case class EnvRuntime(env: EnvName, engine: Engine, workspace: GitWorkspace, evidence: Option[EvidenceRecorder] = None)

/** Who wrote the commit: their email, and their Slack user if the identities map knows them. */
final case class Author(email: String, slack: Option[String])

enum Outcome derives CanEqual:
  /** Nothing left to do at this commit. */
  case Converged(commit: CommitSha)
  case WaitingForPreviousEnv(previous: EnvName)
  case WaitingForApproval(app: AppName, requestTs: String)
  case Stopped(app: AppName, reason: String)
  /** Handed to the executor, which applies it if its own checks pass. */
  case Submitted(app: AppName, plan: String)

/** The policy a reconcile is judged by (the deployed one), and the policy change the target proposes, if any. */
final case class Governance(config: RepoConfig, text: String, proposed: Option[(RepoConfig, String)], deployed: Option[CommitSha] = None):
  def policy: PolicyConfig = config.policy(text)

  /** The facts a plan is judged on: its changes, plus a policy change when the target proposes one. */
  def facts(plan: Plan, author: Option[String], now: java.time.Instant): List[ChangeFacts] =
    val policyFact = Option.when(proposed.isDefined && !plan.isEmpty):
      ChangeFacts(ChangeId("policy"), plan.env, Subject.PolicyText, Risk.High, Set.empty, Origin.Human, author, now)
    ChangeFacts.of(plan, Origin.Human, author, now) ++ policyFact

object Governance:
  /** A change can't loosen its own gate: the policy and the approval settings (owners, groups, production envs,
    * identities) come from the commit deployed in the env, which its resources' `iae:commit` tags say. The apps and
    * channel come from the target. Before the first apply the target's own policy governs. When the target changes
    * the policy or the config, its plans carry a policy change, which the floor gives to the owners.
    */
  def of(runtime: EnvRuntime, apps: List[Manifest], commit: CommitSha, target: RepoConfig, newText: String)
      : IO[AgentError, Governance] =
    for
      deployed <- runtime.engine.deployed(apps.map(_.app), runtime.env).mapError(AgentError.Engine(_))
      governance <- deployed.filterNot(_ == commit) match
                      case None => ZIO.succeed(Governance(target, newText, None, deployed))
                      case Some(sha) =>
                        RepoConfig.loadAt(runtime.workspace, sha).map: (old, oldText) =>
                          val changed = RepoConfig.encode(old.copy(apps = target.apps)) != RepoConfig.encode(target) || oldText.trim != newText.trim
                          Governance(old.copy(apps = target.apps, slack = old.slack.copy(channel = target.slack.channel)), oldText, Option.when(changed)((target, newText)), deployed)
    yield governance


/** Reconciles one env to the head of the branch: expand, then contract, each through the user's policy. Level-
  * triggered and idempotent: running it again after a crash or a dropped event just does what is left.
  */
final case class Pipeline(
    manifests: ManifestSource,
    evaluator: PolicyEvaluator,
    slack: SlackApi,
    self: SlackIdentity,
    approvalExpiry: Duration,
    explainer: Option[PlanExplainer] = None,
    diagnoser: Option[FailureDiagnoser] = None,
    executor: Option[ApplyQueue] = None,
):
  def reconcile(envs: List[EnvRuntime], index: Int): IO[AgentError, Outcome] =
    attempt(envs, index).tapError(failed(envs(index), _))

  /** A failure is announced once per env, commit and kind of failure, with the model's diagnosis in its thread. */
  private def failed(runtime: EnvRuntime, error: AgentError): UIO[Unit] =
    val kind = error match
      case AgentError.Manifest(app, _) => s"building $app"
      case AgentError.Engine(_)        => "planning or applying"
      case AgentError.Git(_)           => "reading the repository"
      case AgentError.Config(_)        => "reading .iae/config.json"
      case AgentError.Slack(_)         => "talking to Slack"
    (for
      head       <- runtime.workspace.git("rev-parse", "HEAD").flatMap(h => ZIO.fromEither(CommitSha.from(h)).mapError(AgentError.Git(_)))
      (config, _) <- RepoConfig.load(runtime.workspace.dir)
      marker      = s"`${runtime.env.value}` at `${head.short}`: $kind failed"
      now        <- Clock.instant
      recent     <- slack.history(config.slack.channel, now.minus(approvalExpiry).nn)
      _ <- ZIO.unless(recent.exists(m => m.botId.contains(self.botId) && m.text.startsWith(s"*Failed* $marker"))):
             for
               ts <- slack.post(config.slack.channel, s"*Failed* $marker\n```\n${error.message.take(2500)}\n```", None, None)
               _ <- ZIO.foreachDiscard(diagnoser): d =>
                      d.diagnose(runtime.workspace, head, runtime.env, error.message)
                        .flatMap(text => slack.post(config.slack.channel, text, Some(ts), None).mapError(_.message))
                        .catchAll(e => ZIO.logWarning(s"diagnosing ${runtime.env.value} at ${head.short}: $e"))
                        .forkDaemon
             yield ()
    yield ()).catchAll(e => ZIO.logWarning(s"announcing a failure in ${runtime.env.value}: ${e.message}"))

  private def attempt(envs: List[EnvRuntime], index: Int): IO[AgentError, Outcome] =
    val runtime = envs(index)
    for
      commit           <- runtime.workspace.sync
      (target, newText) <- RepoConfig.load(runtime.workspace.dir)
      apps             <- manifests.manifests(runtime.workspace.dir, target, commit)
      governance       <- Governance.of(runtime, apps, commit, target, newText)
      authorEmail      <- runtime.workspace.author
      author            = Author(authorEmail, governance.config.identities.get(authorEmail))
      gate             <- ZIO.foreach(Option.when(index > 0)(envs(index - 1)))(previous => converged(previous, apps, commit).map(previous -> _))
      outcome <- gate match
                   case Some((previous, false)) => ZIO.succeed(Outcome.WaitingForPreviousEnv(previous.env))
                   case _                       => applyAll(runtime, governance, author, apps, commit)
      _ <- ZIO.logInfo(s"reconciled ${runtime.env.value} at ${commit.short}: $outcome")
    yield outcome

  /** The previous env is done with this commit when neither phase has anything left to do there. */
  private def converged(previous: EnvRuntime, apps: List[Manifest], commit: CommitSha): IO[AgentError, Boolean] =
    ZIO
      .forall(apps)(m => ZIO.forall(List(Phase.Expand, Phase.Contract))(p => previous.engine.plan(m, previous.env, Some(commit), p).map(_.isEmpty)))
      .mapError(AgentError.Engine(_))

  private def applyAll(runtime: EnvRuntime, governance: Governance, author: Author, apps: List[Manifest], commit: CommitSha): IO[AgentError, Outcome] =
    val approvals = Approvals(slack, self, governance.config.slack.channel, approvalExpiry)
    val steps     = for app <- apps; phase <- List(Phase.Expand, Phase.Contract) yield (app, phase)
    ZIO.foldLeft(steps)(Outcome.Converged(commit)): (outcome, step) =>
      outcome match
        case Outcome.Converged(_) => phase(runtime, governance, author, approvals, step._1, step._2, commit)
        case blocked              => ZIO.succeed(blocked)

  private def phase(
      runtime: EnvRuntime,
      governance: Governance,
      author: Author,
      approvals: Approvals,
      manifest: Manifest,
      phase: Phase,
      commit: CommitSha,
  ): IO[AgentError, Outcome] =
    val config = governance.config
    for
      plan     <- runtime.engine.plan(manifest, runtime.env, Some(commit), phase).mapError(AgentError.Engine(_))
      decision <- decide(governance, plan, author.slack)
      done      = applied(runtime, governance, author, plan, decision, commit, approvals)
      outcome <- (plan.isEmpty, decision.decision) match
                   case (true, _)                => ZIO.succeed(Outcome.Converged(commit))
                   case (_, Decision.Auto)       => done("the policy lets it go ahead", None)
                   case (_, Decision.Deny)       => stopped(config, plan, decision, "the policy denies it")
                   case (_, approve: Decision.Approve) =>
                     approvals.status(plan, decision, approve, author.slack, examples(runtime, governance), explain(config, plan, decision)).flatMap:
                       case ApprovalStatus.Approved(by, request) =>
                         done(s"approved by ${by.toList.sorted.map(u => s"<@$u>").mkString(", ")}", Some(request -> by))
                       case ApprovalStatus.Rejected(by, _) => stopped(config, plan, decision, s"<@$by> rejected it")
                       case ApprovalStatus.Pending(ts)   => ZIO.succeed(Outcome.WaitingForApproval(plan.app, ts))
    yield outcome

  private def decide(governance: Governance, plan: Plan, author: Option[String]): UIO[PlanDecision] =
    val policy = governance.policy
    for
      now      <- Clock.instant
      facts     = governance.facts(plan, author, now)
      verdicts <- evaluator.evaluate(policy, facts).catchAll(e => ZIO.logWarning(s"policy evaluation failed, asking the owners: $e").as(Nil))
    yield Verdicts.decide(facts, verdicts, policy)

  /** The model's explanation, as a reply in the request's thread. Best effort, in the background: an explanation
    * informs approvers but decides nothing, so a failure is only logged.
    */
  private def explain(config: RepoConfig, plan: Plan, decision: PlanDecision)(ts: String): UIO[Unit] =
    ZIO.foreachDiscard(explainer): e =>
      e.explain(plan, decision)
        .flatMap(text => slack.post(config.slack.channel, text, Some(ts), None).mapError(_.message))
        .catchAll(err => ZIO.logWarning(s"explaining plan ${plan.hash.short}: $err"))
        .forkDaemon

  /** For a policy change, how the policy examples fare under the proposed policy, for the owners deciding on it. */
  private def examples(runtime: EnvRuntime, governance: Governance): IO[AgentError, List[String]] =
    ZIO.foreach(governance.proposed.toList): (config, text) =>
      PolicyExamples.load(runtime.workspace.dir)
        .flatMap(PolicyExamples.run(evaluator, governance.policy, config.policy(text), _))
        .map(PolicyExamples.report)

  /** In process, or through the executor when there is one: the agent then only asks, and the executor checks
    * everything again before applying.
    */
  private def applied(runtime: EnvRuntime, governance: Governance, author: Author, plan: Plan, decision: PlanDecision, commit: CommitSha, approvals: Approvals)(
      why: String,
      approval: Option[(SlackMessage, Set[String])],
  ): IO[AgentError, Outcome] =
    executor match
      case Some(queue) =>
        queue
          .submit(ApplyRequest(runtime.env.value, plan.app.value, commit.value, plan.phase.toString, plan.hash.value, approval.map(_._1.ts), author.email))
          .as(Outcome.Submitted(plan.app, plan.hash.short))
      case None =>
        Applying.run(slack, runtime, governance, author, plan, decision, commit, why, approval, approvals).as(Outcome.Converged(commit))

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
