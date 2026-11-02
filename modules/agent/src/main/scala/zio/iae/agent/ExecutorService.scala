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
                else checked(runtime, plan, commit, apps, target, text, request.approval).map(Some(_))
      _ <- inbox.send(InboxEvent.Tick).catchAll(e => ZIO.logWarning(s"executor: waking the agent: $e")).when(report.isDefined)
    yield report

  private def checked(
      runtime: EnvRuntime,
      plan: Plan,
      commit: CommitSha,
      apps: List[Manifest],
      target: RepoConfig,
      text: String,
      approvalTs: Option[String],
  ): IO[String, ApplyReport] =
    for
      governance <- Governance.of(runtime, apps, commit, target, text).mapError(_.message)
      email      <- runtime.workspace.authorOf(commit).mapError(_.message)
      author      = Author(email, governance.config.identities.get(email))
      now        <- Clock.instant
      policy      = governance.policy
      floors      = governance.facts(plan, author.slack, now).map(f => f -> Floor(f, policy))
      floor       = floors.map(_._2._1).foldLeft[Decision](Decision.Auto)(Decision.strictest)
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
      decision = PlanDecision(required, floors.map((f, d) => ChangeDecision(f.id, d._1, d._2)))
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
