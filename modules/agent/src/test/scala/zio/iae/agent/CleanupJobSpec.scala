package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import java.time.Instant
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.EcrImage
import zio.iae.engine.*
import zio.iae.engine.policy.SlackGroup
import zio.iae.testkit.ScriptedModel
import zio.json.ast.Json
import zio.test.*

/** The cleanup job's reports: idle resources (posted once, with the review in the thread) and which old images go. */
object CleanupJobSpec extends ZIOSpecDefault:
  val app     = AppName("photo-app")
  val env     = EnvName("staging")
  val channel = "C_PHOTOS"
  val jobs    = Address(app, env, ResourceKind.Queue, LogicalId("jobs"))
  val uploads = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  /** An engine whose app has an idle queue and a bucket in use, deployed at `commit`. */
  def runtime(workspace: GitWorkspace, commit: CommitSha): EnvRuntime =
    val found: Discovery = (_, _) => ZIO.succeed(List(Discovered(jobs, Some(commit), None), Discovered(uploads, Some(commit), None)))
    val activity: Activity = (address, _) =>
      ZIO.some(if address == jobs then ResourceUsage(jobs, idle = true, "no messages sent in 30 days") else ResourceUsage(uploads, idle = false, "1.2 GB in 40 objects"))
    val engine = Engine.make(Drivers(Nil), found, AnyOrder).copy(activity = activity)
    EnvRuntime(env, engine, workspace)

  def image(tag: String, minutesAgo: Long): EcrImage =
    EcrImage(s"sha256:$tag", List(tag), Some(Instant.parse("2026-11-01T12:00:00Z").nn.minusSeconds(minutesAgo * 60).nn))

  def spec = suite("Cleanup job")(
    test("idle resources are reported once, with the model's review in the thread"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          commit  <- repo.commit(Map("README.md" -> "photos"), "v1")
          ws       = repo.workspace(env)
          _       <- ws.sync.mapError(_.message)
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          model   <- ScriptedModel.make.orDie
          bedrock <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          review  <- ReviewFindings.make(bedrock, "review-cleanup").mapError(_.message)
          _       <- model.reply(
                       ScriptedModel.Turn.ToolUse("search", Json.Obj("pattern" -> Json.Str("Jobs"))),
                       ScriptedModel.Turn.Text("`jobs` is declared in Photos.scala and nothing sends to it any more: remove `MessageQueue.sender(Jobs)`."),
                     )
          report   = IdleReport(slack, slack.self, channel, Some(review))
          _       <- report.check(runtime(ws, commit), app)
          _       <- report.check(runtime(ws, commit), app)
          posted  <- slack.posted(channel)
          notes   <- slack.threadTexts(channel, posted.head.ts).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(50)).map(_._2)
        yield assertTrue(
          posted.size == 1,
          posted.head.text.startsWith(s"*Cleanup* `photo-app/staging`: nothing has used this resource in 30 days"),
          posted.head.text.contains("- Queue `jobs`: no messages sent in 30 days"),
          !posted.head.text.contains("uploads"),
          notes.head.contains("remove `MessageQueue.sender(Jobs)`"),
        ),
    test("only images older than every image in use, beyond the newest few and not in use, are deleted"):
      val images = List(image("c9", 0), image("c8", 10), image("c7", 20), image("c6", 30), image("c5", 40), image("c4", 50), image("c3", 60))
      // Prod runs c7 and would roll back to c5; c8 and c9 are waiting to go out.
      val inUse  = Set("c7", "c5")
      val doomed = ImagePruner.doomed(images, inUse, keep = 2).map(_.tags.head)
      assertTrue(
        doomed == List("c4", "c3"),
        ImagePruner.doomed(images, Set.empty, keep = 0).isEmpty,
        ImagePruner.tagOf("123.dkr.ecr.us-east-1.amazonaws.com/iae/photo-app:c7").contains("c7"),
        ImagePruner.tagOf("localhost:4566/000000000000/us-east-1/iae/photo-app:c7").contains("c7"),
        ImagePruner.tagOf("repo/iae/photo-app@sha256:abc").contains("sha256:abc"),
        ImagePruner.tagOf("hashicorp/http-echo").isEmpty,
      )
    ,
  ).provideShared(Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
