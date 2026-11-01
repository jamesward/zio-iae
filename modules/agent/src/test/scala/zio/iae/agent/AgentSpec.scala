package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.AwsError
import zio.iae.engine.*
import zio.iae.engine.policy.*
import zio.iae.testkit.FlociLayer
import zio.test.*

object AgentSpec extends ZIOSpecDefault:
  object Uploads          extends Bucket.Id(LogicalId("uploads"))
  object UploadsVersioned extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))

  val app       = AppName("photo-app")
  val channel   = "C_PHOTOS"
  val approvers = "infra-approvers"
  val owners    = "platform-owners"

  val uniqueEnv: UIO[EnvName] =
    ZIO.collectAll(List.fill(6)(Random.nextIntBounded(26).map(i => ('a' + i).toChar))).map(cs => EnvName.from("a" + cs.mkString).toOption.get)

  def manifestJson(infra: Infra[?]): String = infra.manifest(app).toOption.get.toJson

  def configJson(staging: EnvName, prod: EnvName): String =
    s"""{"apps":[{"name":"photo-app","manifest":["cat",".iae/manifest.json"]}],
       |"envs":["${staging.value}","${prod.value}"],"production":["${prod.value}"],
       |"slack":{"channel":"$channel","owners":"$owners","groups":["$approvers"]},
       |"identities":{"dev@example.com":"U_DEV"}}""".stripMargin

  final case class World(repo: TestRepo, slack: InMemorySlack, envs: List[EnvRuntime], pipeline: Pipeline)

  def world: ZIO[Scope & AwsEngine.Clients, String, World] =
    for
      repo    <- TestRepo.make
      staging <- uniqueEnv
      prod    <- uniqueEnv
      slack   <- InMemorySlack.make(Map(approvers -> Set("U_ALICE", "U_DEV"), owners -> Set("U_BOB")), bots = Set("U_OTHER_BOT"))
      _       <- repo.commit(Map(".iae/config.json" -> configJson(staging, prod), ".iae/manifest.json" -> manifestJson(Bucket.readWrite(Uploads))), "v1")
      envs <- ZIO.foreach(List(staging, prod)): env =>
                val workspace = repo.workspace(env)
                AwsEngine
                  .layer(BucketNaming(None))
                  .build
                  .provideSome[AwsEngine.Clients & Scope](ZLayer.succeed[CommitOrder](GitCommitOrder(workspace.dir)))
                  .map(e => EnvRuntime(env, e.get, workspace))
      pipeline = Pipeline(CommandManifests(), RiskTableEvaluator(SlackGroup.from(approvers).toOption.get), slack, slack.self, 24.hours)
    yield World(repo, slack, envs, pipeline)

  def requests(slack: InMemorySlack): UIO[List[SlackMessage]] = slack.posted(channel).map(_.filter(_.request.isDefined))

  def spec = suite("Agent on Floci")(
    test("a push converges staging, then prod; a gated prod change waits for an eligible approver"):
      ZIO.scoped:
        for
          w       <- world
          v1s     <- w.pipeline.reconcile(w.envs, 0)
          v1p     <- w.pipeline.reconcile(w.envs, 1)
          v2       <- w.repo.commit(Map(".iae/manifest.json" -> manifestJson(Bucket.readWrite(UploadsVersioned))), "turn on versioning")
          early   <- w.pipeline.reconcile(w.envs, 1)
          v2s     <- w.pipeline.reconcile(w.envs, 0)
          waiting <- w.pipeline.reconcile(w.envs, 1)
          request <- requests(w.slack).map(_.head)
          _       <- w.slack.react(request.ts, "U_CAROL", Approvals.approveReaction)
          _       <- w.slack.react(request.ts, "U_DEV", Approvals.approveReaction)
          _       <- w.slack.react(request.ts, "U_OTHER_BOT", Approvals.approveReaction)
          still   <- w.pipeline.reconcile(w.envs, 1)
          again   <- requests(w.slack)
          _       <- w.slack.react(request.ts, "U_ALICE", Approvals.approveReaction)
          done    <- w.pipeline.reconcile(w.envs, 1)
          posted  <- w.slack.posted(channel)
        yield assertTrue(
          v1s.isInstanceOf[Outcome.Converged],
          v1p.isInstanceOf[Outcome.Converged],
          early == Outcome.WaitingForPreviousEnv(w.envs.head.env),
          v2s == Outcome.Converged(v2),
          waiting.isInstanceOf[Outcome.WaitingForApproval],
          request.request.exists(r => r.env == w.envs(1).env.value && r.author.contains("U_DEV") && r.count == 1),
          request.text.contains("needs 1 approval(s) from @infra-approvers"),
          still == waiting,
          again.size == 1,
          done == Outcome.Converged(v2),
          posted.exists(m => m.text.startsWith("Applied") && m.text.contains("<@U_ALICE>")),
        )
    ,
    test("a rejection stops the change and is announced once; a restarted agent has nothing to do"):
      ZIO.scoped:
        for
          w        <- world
          _        <- w.pipeline.reconcile(w.envs, 0)
          _        <- w.pipeline.reconcile(w.envs, 1)
          _        <- w.repo.commit(Map(".iae/manifest.json" -> manifestJson(Bucket.readWrite(UploadsVersioned))), "turn on versioning")
          _        <- w.pipeline.reconcile(w.envs, 0)
          _        <- w.pipeline.reconcile(w.envs, 1)
          request  <- requests(w.slack).map(_.head)
          _        <- w.slack.react(request.ts, "U_BOB", Approvals.rejectReaction)
          ignored  <- w.pipeline.reconcile(w.envs, 1)
          _        <- w.slack.react(request.ts, "U_ALICE", Approvals.rejectReaction)
          rejected <- w.pipeline.reconcile(w.envs, 1)
          _        <- w.pipeline.reconcile(w.envs, 1)
          notices  <- w.slack.posted(channel).map(_.filter(_.text.startsWith("Not applying")))
          restarted = w.pipeline.copy(evaluator = RiskTableEvaluator(SlackGroup.from(approvers).toOption.get))
          staging  <- restarted.reconcile(w.envs, 0)
        yield assertTrue(
          ignored.isInstanceOf[Outcome.WaitingForApproval],
          rejected match
            case Outcome.Stopped(_, why) => why.contains("U_ALICE")
            case _                       => false
          ,
          notices.size == 1,
          staging.isInstanceOf[Outcome.Converged],
        )
    ,
    test("the reconcile loops promote a push from staging to prod on their own"):
      ZIO.scoped:
        for
          w           <- world
          reconcilers <- Reconcilers.make(w.pipeline, w.envs)
          _           <- reconcilers.run.forkScoped
          _           <- reconcilers.signals.head.offer(())
          manifest     = Bucket.readWrite(Uploads).manifest(app).toOption.get
          prod         = w.envs(1)
          converged    = ZIO.forall(List(Phase.Expand, Phase.Contract))(p => prod.engine.plan(manifest, prod.env, None, p).map(_.isEmpty)).orElseSucceed(false)
          done        <- converged.repeat(Schedule.spaced(500.millis) && Schedule.recurUntil[Boolean](identity)).timeout(2.minutes)
        yield assertTrue(done.exists(_._2))
    ,
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >>> AwsEngine.clients).mapError(e => e match
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
    )
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
