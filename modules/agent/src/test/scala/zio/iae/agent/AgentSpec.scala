package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.{BucketNaming, CloudTrailAttribution}
import zio.iae.aws.client.{AwsConfig, AwsError, AwsJsonClient, CloudTrailClient, Endpoint, S3Client}
import zio.iae.engine.*
import zio.iae.engine.policy.*
import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.iae.testkit.{CloudTrailFake, FlociLayer, ScriptedModel}
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
    test("a hand change in staging is reported as drift, naming who made it, and reverted"):
      ZIO.scoped:
        for
          w       <- world
          _       <- w.pipeline.reconcile(w.envs, 0)
          fake    <- CloudTrailFake.make.orDie
          http    <- ZIO.service[Client]
          aws     <- ZIO.service[AwsConfig]
          trail    = CloudTrailClient(AwsJsonClient(http, aws.copy(endpoint = Endpoint.parse(fake.url.encode).toOption)))
          staging  = w.envs.head.copy(engine = w.envs.head.engine.copy(attribution = CloudTrailAttribution(trail, BucketNaming(None), Some("iae-deployer"))))
          address  = Address(app, staging.env, ResourceKind.Bucket, LogicalId("uploads"))
          bucket  <- ZIO.fromEither(BucketNaming(None).physical(address)).mapError(_.message)
          now     <- Clock.instant
          // Someone turns versioning on in the console.
          _       <- S3Client(http, aws).putBucketVersioning(bucket, enabled = true).mapError(_.toString)
          _       <- fake.record(bucket.value, "PutBucketVersioning", "arn:aws:sts::000000000000:assumed-role/Admin/carol", now)
          revert  <- w.pipeline.reconcile(staging :: w.envs.tail, 0)
          notices <- w.slack.posted(channel).map(_.filter(_.text.startsWith("*Drift*")))
        yield assertTrue(
          revert.isInstanceOf[Outcome.Converged],
          notices.size == 1,
          notices.head.text.contains("Bucket uploads"),
          notices.head.text.contains("PutBucketVersioning by `arn:aws:sts::000000000000:assumed-role/Admin/carol`"),
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
    test("POLICY.md is evaluated by the model: a denied change is stopped and the clause is quoted"):
      val clause = "Never create buckets in staging."
      ZIO.scoped:
        for
          w       <- world
          _       <- w.repo.commit(Map(RepoConfig.policyPath -> s"# Approval policy\n\n- $clause\n- Roles are fine.\n"), "policy")
          model   <- ScriptedModel.make.orDie
          staging  = w.envs.head.env.value
          _       <- model.replyText(
                       s"""{"verdicts":[
                          |{"change":"0:photo-app/$staging/Bucket/uploads","decision":"Deny","approvals":0,"groups":[],"clauses":["$clause"],"reasoning":"a new bucket in staging","confident":true},
                          |{"change":"1:photo-app/$staging/Role/runtime","decision":"Auto","approvals":0,"groups":[],"clauses":["Roles are fine."],"reasoning":"a role","confident":true}
                          |]}""".stripMargin,
                     )
          eval    <- Model.make(ConverseConfig(ApiKey("test-key"), model.url, ModelId("scripted"))).flatMap(EvaluatePolicy.make).mapError(_.message)
          outcome <- w.pipeline.copy(evaluator = eval).reconcile(w.envs, 0)
          notices <- w.slack.posted(channel).map(_.filter(_.text.startsWith("Not applying")))
          asked   <- model.requests
        yield assertTrue(
          outcome match
            case Outcome.Stopped(_, why) => why.contains("denies")
            case _                       => false
          ,
          notices.size == 1,
          notices.head.text.contains("a new bucket in staging"),
          asked.size == 1,
        )
    ,
    test("a push that changes the policy or config needs an owner and is judged by the deployed config"):
      ZIO.scoped:
        for
          w        <- world
          _        <- w.pipeline.reconcile(w.envs, 0)
          loosened  = configJson(w.envs(0).env, w.envs(1).env).replace("\"dev@example.com\":\"U_DEV\"", "")
          example   = PolicyExamplesSpec.exampleJson(Risk.Medium, Decision.Auto)
          _        <- w.repo.commit(
                        Map(
                          ".iae/config.json"                         -> loosened,
                          ".iae/manifest.json"                       -> manifestJson(Bucket.readWrite(UploadsVersioned)),
                          s"${PolicyExamples.dir}/medium-is-fine.json" -> example,
                        ),
                        "versioning, and forget who dev is",
                      )
          waiting  <- w.pipeline.reconcile(w.envs, 0)
          request  <- requests(w.slack).map(_.head)
          _        <- w.slack.react(request.ts, "U_BOB", Approvals.approveReaction)
          ownerOnly <- w.pipeline.reconcile(w.envs, 0)
          _        <- w.slack.react(request.ts, "U_ALICE", Approvals.approveReaction)
          approved <- w.pipeline.reconcile(w.envs, 0)
          v3       <- w.repo.commit(Map(".iae/manifest.json" -> manifestJson(Bucket.readWrite(Uploads))), "versioning off again")
          next     <- w.pipeline.reconcile(w.envs, 0)
          all      <- requests(w.slack)
        yield assertTrue(
          waiting.isInstanceOf[Outcome.WaitingForApproval],
          request.text.contains("from each of @infra-approvers, @platform-owners"),
          ownerOnly.isInstanceOf[Outcome.WaitingForApproval],
          request.text.contains(":white_check_mark: `medium-is-fine`: can go ahead"),
          request.request.exists(_.author.contains("U_DEV")),
          approved.isInstanceOf[Outcome.Converged],
          next == Outcome.Converged(v3),
          all.size == 1,
        )
    ,
    test("a new approval request gets the model's explanation in its thread"):
      object Explainer extends PlanExplainer:
        def explain(plan: Plan, decision: PlanDecision): IO[String, String] = ZIO.succeed(s"Plan ${plan.hash.short} turns on versioning.")
      ZIO.scoped:
        for
          w        <- world
          pipeline  = w.pipeline.copy(explainer = Some(Explainer))
          _        <- pipeline.reconcile(w.envs, 0)
          _        <- pipeline.reconcile(w.envs, 1)
          _        <- w.repo.commit(Map(".iae/manifest.json" -> manifestJson(Bucket.readWrite(UploadsVersioned))), "turn on versioning")
          _        <- pipeline.reconcile(w.envs, 0)
          _        <- pipeline.reconcile(w.envs, 1)
          request  <- requests(w.slack).map(_.head)
          _        <- w.slack.threadTexts(channel, request.ts).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(50))
          replies  <- w.slack.threadTexts(channel, request.ts)
          _        <- pipeline.reconcile(w.envs, 1)
          again    <- w.slack.threadTexts(channel, request.ts)
        yield assertTrue(replies.size == 1, replies.head.contains("turns on versioning"), again.size == 1)
    ,
    test("cleanup orphans an app the config no longer declares, but not an app from another repo"):
      ZIO.scoped:
        for
          w       <- world
          staging  = w.envs.head
          old      = AppName("old-app")
          other    = AppName("their-app")
          both     = configJson(w.envs(0).env, w.envs(1).env).replace(
                       "\"apps\":[", "\"apps\":[{\"name\":\"old-app\",\"manifest\":[\"cat\",\".iae/old.json\"]},")
          _       <- w.repo.commit(Map(".iae/config.json" -> both, ".iae/old.json" -> Bucket.readWrite(Uploads).manifest(old).toOption.get.toJson), "add old-app")
          _       <- w.pipeline.reconcile(w.envs, 0)
          _       <- w.repo.commit(Map(".iae/config.json" -> configJson(w.envs(0).env, w.envs(1).env)), "drop old-app")
          kept    <- w.pipeline.reconcile(w.envs, 0)
          stays   <- staging.engine.commits(old, staging.env)
          // Another repo's app in the same account: applied at a commit this repo doesn't have.
          theirs  <- ZIO.fromEither(Bucket.readWrite(Uploads).manifest(other)).mapError(_.toString)
          _       <- staging.engine.converge(theirs, staging.env, Some(CommitSha.from("f" * 40).toOption.get)).mapError(_.message)
          found   <- staging.engine.apps(staging.env).mapError(_.message)
          job      = CleanupJob(List(staging), List(app), w.pipeline.cleanup)
          _       <- job.sweep
          posted  <- w.slack.posted(channel)
          orphans <- staging.engine.cleanup(old, staging.env, java.time.LocalDate.now().nn.plusDays(1).nn).mapError(_.message)
          ignored <- w.pipeline.cleanup(staging, other).mapError(_.message)
        yield assertTrue(
          kept.isInstanceOf[Outcome.Converged],
          stays.nonEmpty,
          found == Set(app, old, other),
          posted.exists(m => m.text.startsWith("Applied") && m.text.contains(s"old-app/${staging.env.value}") && m.text.contains("orphan  Bucket uploads")),
          !posted.exists(_.text.contains("their-app")),
          // Orphaned today, so a cutoff of tomorrow would delete it.
          orphans.changes.map(_.change.productPrefix).toSet == Set("Delete"),
          ignored.isEmpty,
        ),
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
    ((FlociLayer.live ++ Client.default) >>> (AwsEngine.clients ++ ZLayer.environment[Client & AwsConfig])).mapError(e => e match
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
    )
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
