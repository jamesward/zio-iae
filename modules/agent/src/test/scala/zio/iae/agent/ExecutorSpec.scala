package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.AwsError
import zio.iae.engine.*
import zio.iae.testkit.FlociLayer
import zio.iae.webhook.Inbox
import zio.test.*

/** The agent with an executor: the agent only asks; the executor checks again and applies. */
object ExecutorSpec extends ZIOSpecDefault:
  import AgentSpec.{channel, configJson, manifestJson, requests, Uploads, UploadsVersioned}

  final case class Setup(w: AgentSpec.World, agent: Pipeline, submitted: Queue[ApplyRequest], executor: ExecutorService, woken: Ref[Chunk[InboxEvent]])

  val setup: ZIO[Scope & AwsEngine.Clients, String, Setup] =
    for
      w         <- AgentSpec.world
      submitted <- Queue.unbounded[ApplyRequest]
      woken     <- Ref.make(Chunk.empty[InboxEvent])
      queue      = new ApplyQueue:
                     def submit(r: ApplyRequest): IO[AgentError, Unit] = submitted.offer(r).unit
      inbox      = new Inbox:
                     def send(e: InboxEvent): IO[String, Unit] = woken.update(_ :+ e)
      // The executor's own checkouts of the repo, with the same engines (one account in this test).
      envs       = w.envs.map(r => r.copy(workspace = GitWorkspace(w.repo.root.resolve("executor").nn.resolve(r.env.value).nn, w.repo.remote.toString, "main")))
      executor   = ExecutorService(w.slack, w.slack.self, envs, CommandManifests(), inbox, 24.hours)
    yield Setup(w, w.pipeline.copy(executor = Some(queue)), submitted, executor, woken)

  def run(s: Setup, index: Int): IO[String, (Outcome, Option[ApplyRequest])] =
    for
      outcome <- s.agent.reconcile(s.w.envs, index).mapError(_.message)
      request <- s.submitted.poll
      _       <- ZIO.foreachDiscard(request)(s.executor.handle)
    yield (outcome, request)

  def refusals(s: Setup): UIO[List[String]] = s.w.slack.posted(channel).map(_.map(_.text).filter(_.startsWith("*Refused*")))

  def spec = suite("Executor")(
    test("the agent asks and the executor applies: staging on its own, prod after a verified approval"):
      ZIO.scoped:
        for
          s        <- setup
          (seed, r1) <- run(s, 0)
          (again, _) <- run(s, 0)
          _        <- run(s, 1)
          _        <- run(s, 1)
          _        <- s.w.repo.commit(Map(".iae/manifest.json" -> manifestJson(Bucket.readWrite(UploadsVersioned))), "versioning")
          _        <- run(s, 0)
          _        <- run(s, 0)
          (waiting, none) <- run(s, 1)
          request  <- requests(s.w.slack).map(_.head)
          _        <- s.w.slack.react(request.ts, "U_ALICE", Approvals.approveReaction)
          (asked, r2) <- run(s, 1)
          (done, _) <- run(s, 1)
          posted   <- s.w.slack.posted(channel)
          woken    <- s.woken.get
        yield assertTrue(
          seed.isInstanceOf[Outcome.Submitted],
          r1.exists(_.approval.isEmpty),
          again.isInstanceOf[Outcome.Converged],
          waiting.isInstanceOf[Outcome.WaitingForApproval],
          none.isEmpty,
          asked.isInstanceOf[Outcome.Submitted],
          r2.flatMap(_.approval).contains(request.ts),
          done.isInstanceOf[Outcome.Converged],
          posted.exists(m => m.text.startsWith("Applied") && m.text.contains("<@U_ALICE>") && m.text.contains("checked by the executor")),
          posted.find(_.ts == request.ts).exists(_.text.startsWith(Approvals.Approved)),
          woken.nonEmpty && woken.forall(_ == InboxEvent.Tick),
        ),
    test("the executor refuses what its own checks don't allow, and says so once"):
      ZIO.scoped:
        for
          s         <- setup
          _         <- run(s, 0)
          // A policy change: the floor wants an owner, whatever the agent's verdict was.
          loosened   = configJson(s.w.envs(0).env, s.w.envs(1).env).replace("\"dev@example.com\":\"U_DEV\"", "")
          _         <- s.w.repo.commit(Map(".iae/config.json" -> loosened, ".iae/manifest.json" -> manifestJson(Bucket.readWrite(UploadsVersioned))), "loosen")
          _         <- s.agent.reconcile(s.w.envs, 0).mapError(_.message)
          request   <- requests(s.w.slack).map(_.last)
          meta       = request.request.get
          head      <- s.w.envs(0).workspace.git("rev-parse", "HEAD").mapError(_.message)
          noApproval = ApplyRequest(s.w.envs(0).env.value, "photo-app", head, "Expand", meta.plan, None, "dev@example.com")
          _         <- s.executor.handle(noApproval)
          _         <- s.executor.handle(noApproval)
          chatter   <- s.w.slack.say(channel, "U_ALICE", "looks fine")
          _         <- s.executor.handle(noApproval.copy(approval = Some(chatter)))
          _         <- s.executor.handle(noApproval.copy(plan = "0" * 64))
          _         <- s.executor.handle(noApproval.copy(commit = "f" * 40))
          refused   <- refusals(s)
          applied   <- s.w.slack.posted(channel).map(_.filter(_.text.startsWith("Applied")))
        yield assertTrue(
          refused.count(_.contains("there's no approval request")) == 1,
          refused.exists(_.contains("isn't an approval request")),
          refused.exists(_.contains("the plan is now")),
          refused.exists(_.contains("isn't on main")) || refused.exists(_.contains("ffffff")),
          applied.size == 1,
        ),
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >>> AwsEngine.clients).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes) @@ TestAspect.sequential
