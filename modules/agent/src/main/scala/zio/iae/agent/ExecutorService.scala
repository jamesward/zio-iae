package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.aws.client.{QueueUrl, SqsClient}
import zio.iae.engine.*
import zio.iae.engine.policy.*
import zio.iae.webhook.Inbox

/** The executor: the only principal that applies, in its own process with its own credentials (the deployer roles)
  * and its own Slack token, and no model. The agent can only ask it to apply a plan; the executor trusts the request
  * only to say which plan. It checks out the commit itself (which must be on the branch), recomputes the plan (which
  * must have the same hash), the policy deployed in the env and the built-in floor, and verifies the approval request
  * and its reactions in Slack. What the policy text alone requires is the agent's verdict, carried by the approval
  * request it posted; the floor and the reactions are checked here, deterministically.
  */
final case class ExecutorService(
    slack: SlackApi,
    agent: SlackIdentity,
    envs: List[EnvRuntime],
    manifests: ManifestSource,
    inbox: Inbox,
    approvalExpiry: Duration,
):
  def handle(request: ApplyRequest): UIO[Unit] =
    execute(request).foldZIO(reason => refuse(request, reason), _ => ZIO.unit)

  /** Reads the executor's queue. Each env has a lane of its own, where requests are handled one at a time (an env's
    * workspace is checked out at one commit at a time), so a canary baking in one env doesn't hold up the others. A
    * request already waiting in its lane is dropped: the agent asks again on each reconcile until the change is in.
    * Messages are deleted once they're in a lane; if the executor stops first, the agent's next reconcile asks again.
    */
  def consume(sqs: SqsClient, url: QueueUrl): UIO[Nothing] =
    for
      lanes <- ZIO.foreach(envs)(r => Lane.make(handle).map(r.env.value -> _)).map(_.toMap)
      other <- Lane.make(handle)
      _     <- ZIO.foreachDiscard(other :: lanes.values.toList)(_.run.fork)
      never <- sqs.receive(url, 10, 20)
                 .flatMap: messages =>
                   ZIO.foreachDiscard(messages): message =>
                     (ApplyRequest.decode(message.body) match
                       case Left(e)        => ZIO.logWarning(s"executor: ignoring an unreadable request: $e")
                       case Right(request) => lanes.getOrElse(request.env, other).offer(request)
                     ) *> sqs.delete(url, message.receipt)
                 .catchAllCause(c => ZIO.logErrorCause("executor: reading the queue failed", c).delay(5.seconds))
                 .forever
    yield never

  /** Applies the plan if every check passes; None if there's nothing left to apply. */
  def execute(request: ApplyRequest): IO[String, Option[ApplyReport]] =
    (request.rollback, request.cleanup) match
      case (Some(service), _) => executeRollback(request, service)
      case (_, Some(cutoff))  => executeCleanup(request, cutoff)
      case _                  => executePlan(request)

  /** A cleanup: planned here from the env (orphans no newer than the deployed config's retention allows, old task
    * definitions beyond what it keeps, and an app it no longer declares, if the app's resources come from this
    * repo), judged by the policy deployed in the env as `origin = Cleanup`.
    */
  private def executeCleanup(request: ApplyRequest, cutoff: String): IO[String, Option[ApplyReport]] =
    for
      runtime  <- ZIO.fromOption(envs.find(_.env.value == request.env)).orElseFail(s"no env ${request.env}")
      app      <- ZIO.fromEither(AppName.from(request.app)).mapError(_.toString)
      before   <- ZIO.attempt(java.time.LocalDate.parse(cutoff).nn).orElseFail(s"$cutoff isn't a date")
      basis    <- Cleaning.basis(runtime, app).mapError(_.message).someOrFail(s"nothing of ${request.app} from this repo is deployed in ${request.env}")
      _        <- ZIO.fail(s"${request.commit.take(7)} isn't the commit deployed in ${request.env}").unless(basis.commit.value == request.commit)
      today    <- Clock.localDateTime.map(_.toLocalDate.nn)
      _        <- ZIO.fail(s"orphans since $before haven't been kept ${basis.config.orphanRetentionDays} days").when(before.isAfter(basis.cutoff(today)))
      plan     <- runtime.engine.cleanup(app, runtime.env, before, basis.config.keepVersions, basis.abandoned).mapError(_.message)
      report <- if plan.isEmpty then ZIO.none
                else if plan.hash.value != request.plan then ZIO.fail(s"the cleanup is now ${plan.hash.short}, not ${request.plan.take(12)}")
                else checked(runtime, plan, basis.commit, Governance(basis.config, basis.text, None, Some(basis.commit)), Pipeline.agentAuthor, Origin.Cleanup, request.approval).map(Some(_))
      _ <- inbox.send(InboxEvent.Tick).catchAll(e => ZIO.logWarning(s"executor: waking the agent: $e")).when(report.isDefined)
    yield report

  /** A rollback: planned here from what's live, judged by the policy deployed in the env, as `origin = Alert`. */
  private def executeRollback(request: ApplyRequest, service: String): IO[String, Option[ApplyReport]] =
    for
      runtime  <- ZIO.fromOption(envs.find(_.env.value == request.env)).orElseFail(s"no env ${request.env}")
      app      <- ZIO.fromEither(AppName.from(request.app)).mapError(_.toString)
      id       <- ZIO.fromEither(LogicalId.from(service)).mapError(_.toString)
      deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
      commit   <- ZIO.fromOption(deployed.filter(_.value == request.commit)).orElseFail(s"${request.commit.take(7)} isn't the commit deployed in ${request.env}")
      _        <- runtime.workspace.sync.mapError(_.message)
      (config, text) <- RepoConfig.loadAt(runtime.workspace, commit).mapError(_.message)
      plan     <- runtime.engine.rollback(Address(app, runtime.env, ResourceKind.Service, id)).mapError(_.message)
      report <- if plan.isEmpty then ZIO.none
                else if plan.hash.value != request.plan then ZIO.fail(s"the rollback is now ${plan.hash.short}, not ${request.plan.take(12)}")
                else checked(runtime, plan, commit, Governance(config, text, None, deployed), Pipeline.agentAuthor, Origin.Alert, request.approval).map(Some(_))
      _ <- inbox.send(InboxEvent.Tick).catchAll(e => ZIO.logWarning(s"executor: waking the agent: $e")).when(report.isDefined)
    yield report

  private def executePlan(request: ApplyRequest): IO[String, Option[ApplyReport]] =
    for
      runtime  <- ZIO.fromOption(envs.find(_.env.value == request.env)).orElseFail(s"no env ${request.env}")
      commit   <- ZIO.fromEither(CommitSha.from(request.commit))
      phase    <- ZIO.fromOption(Phase.values.find(_.toString == request.phase)).orElseFail(s"no phase ${request.phase}")
      head     <- runtime.workspace.sync.mapError(_.message)
      onBranch <- runtime.workspace.contains(commit, head).mapError(_.message)
      _        <- ZIO.fail(s"commit ${commit.short} isn't on ${runtime.workspace.branch}").unless(onBranch)
      _        <- runtime.workspace.checkout(commit).mapError(_.message)
      (target, text) <- RepoConfig.load(runtime.workspace.dir).mapError(_.message)
      apps     <- manifests.manifests(runtime.workspace.dir, target, commit).mapError(_.message)
      manifest <- ZIO.fromOption(apps.find(_.app.value == request.app)).orElseFail(s"no app ${request.app} at ${commit.short}")
      plan     <- runtime.engine.plan(manifest, runtime.env, Some(commit), phase).mapError(_.message)
      report <- if plan.isEmpty then ZIO.none
                else if plan.hash.value != request.plan then ZIO.fail(s"the plan is now ${plan.hash.short}, not ${request.plan.take(12)}; the agent will ask again")
                else
                  for
                    governance <- Governance.of(runtime, apps, commit, target, text).mapError(_.message)
                    email      <- runtime.workspace.authorOf(commit).mapError(_.message)
                    report     <- checked(runtime, plan, commit, governance, Author(email, governance.config.identities.get(email)), Origin.Human, request.approval)
                  yield Some(report)
      _ <- inbox.send(InboxEvent.Tick).catchAll(e => ZIO.logWarning(s"executor: waking the agent: $e")).when(report.isDefined)
    yield report

  private def checked(
      runtime: EnvRuntime,
      plan: Plan,
      commit: CommitSha,
      governance: Governance,
      author: Author,
      origin: Origin,
      approvalTs: Option[String],
  ): IO[String, ApplyReport] =
    for
      now        <- Clock.instant
      policy      = governance.policy
      facts       = governance.facts(plan, author.slack, now, origin)
      floors      = facts.map(f => f -> Floor(f, policy))
      whole       = Floor.plan(facts, policy)
      floor       = (floors.map(_._2._1) ++ whole.map(_.decision)).foldLeft[Decision](Decision.Auto)(Decision.strictest)
      channel     = governance.config.slack.channel
      approvals   = Approvals(slack, agent, channel, approvalExpiry)
      request    <- ZIO.foreach(approvalTs)(ts => requestFor(channel, ts, plan, now))
      required    = request.fold(floor)((_, asked) => Decision.strictest(floor, asked))
      approved <- required match
                    case Decision.Auto => ZIO.none
                    case Decision.Deny => ZIO.fail("the built-in floor denies it")
                    case approve: Decision.Approve =>
                      ZIO.fromOption(request).orElseFail(s"it ${PlanText.describe(approve)} and there's no approval request").flatMap: (message, _) =>
                        approvals.verify(message, approve, author.slack).mapError(_.message).flatMap:
                          case ApprovalStatus.Approved(by, m) => ZIO.some(m -> by)
                          case ApprovalStatus.Rejected(by, _) => ZIO.fail(s"<@$by> rejected it")
                          case ApprovalStatus.Pending(_)      => ZIO.fail(s"it ${PlanText.describe(approve)}, which the request doesn't have")
      decision = PlanDecision(required, floors.map((f, d) => ChangeDecision(f.id, d._1, d._2)) ++ whole.toList)
      why      = approved.fold("the policy lets it go ahead")((_, by) => s"approved by ${by.toList.sorted.map(u => s"<@$u>").mkString(", ")}") + "; checked by the executor"
      report  <- Applying.run(slack, runtime, governance, author, plan, decision, commit, why, approved, approvals).mapError(_.message)
    yield report

  /** The approval request the agent pointed at, if it is one: posted by the agent's bot, still open and unexpired,
    * for this app, env and manifest, and covering every change of the plan. Its count and groups are what it asks for.
    */
  private def requestFor(channel: String, ts: String, plan: Plan, now: java.time.Instant): IO[String, (SlackMessage, Decision.Approve)] =
    for
      thread  <- slack.replies(channel, ts).mapError(_.message)
      message <- ZIO.fromOption(thread.headOption.filter(_.ts == ts)).orElseFail(s"no message $ts in the channel")
      r       <- ZIO.fromOption(message.request).orElseFail(s"message $ts isn't an approval request")
      _       <- ZIO.fail(s"message $ts wasn't posted by the agent").unless(message.botId.contains(agent.botId))
      _       <- ZIO.fail(s"request $ts is closed").when(Approvals.closed(message.text))
      _       <- ZIO.fail(s"request $ts has expired").when(r.expires <= now.getEpochSecond)
      _ <- ZIO.fail(s"request $ts is for a different plan")
             .unless(r.app == plan.app.value && r.env == plan.env.value && r.manifest == plan.manifest.value && Approvals.keys(plan).subsetOf(r.changes.toSet))
      groups <- ZIO.foreach(r.groups)(g => ZIO.fromEither(SlackGroup.from(g)))
    yield (message, Decision.Approve(r.count.max(1), groups.toSet))

  /** A refusal is announced once per plan and reason. */
  private def refuse(request: ApplyRequest, reason: String): UIO[Unit] =
    val text = s"*Refused* by the executor: ${request.app}/${request.env} plan ${request.plan.take(12)}: $reason"
    (for
      _       <- ZIO.logWarning(text)
      runtime <- ZIO.fromOption(envs.find(_.env.value == request.env)).orElseFail("no env")
      config  <- RepoConfig.load(runtime.workspace.dir).mapBoth(_.message, _._1)
      now     <- Clock.instant
      recent  <- slack.history(config.slack.channel, now.minus(approvalExpiry).nn).mapError(_.message)
      _       <- slack.post(config.slack.channel, text, None, None).mapError(_.message).unless(recent.exists(_.text == text))
    yield ()).catchAll(e => ZIO.logWarning(s"executor: announcing a refusal: $e"))

/** One env's requests, handled in order; a request already waiting is not queued twice. */
final case class Lane(queue: Queue[ApplyRequest], waiting: Ref[Set[(String, String)]], handle: ApplyRequest => UIO[Unit]):
  private def key(r: ApplyRequest) = (r.app, r.plan)

  def offer(request: ApplyRequest): UIO[Unit] =
    waiting.modify(w => if w(key(request)) then (false, w) else (true, w + key(request)))
      .flatMap(fresh => queue.offer(request).unit.when(fresh))
      .unit

  def run: UIO[Nothing] =
    queue.take.flatMap(r => waiting.update(_ - key(r)) *> handle(r)).forever

object Lane:
  def make(handle: ApplyRequest => UIO[Unit]): UIO[Lane] =
    for
      queue   <- Queue.unbounded[ApplyRequest]
      waiting <- Ref.make(Set.empty[(String, String)])
    yield Lane(queue, waiting, handle)
