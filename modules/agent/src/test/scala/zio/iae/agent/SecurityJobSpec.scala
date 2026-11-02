package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.aws.client.{AwsConfig, AwsError}
import zio.iae.engine.*
import zio.iae.testkit.{FlociLayer, ScriptedModel}
import zio.json.ast.Json
import zio.test.*

/** The security auditor on Floci, which records no usage: with a zero window, every permission of a new role is
  * unused, posted once, and reviewed in its thread.
  */
object SecurityJobSpec extends ZIOSpecDefault:
  def spec = suite("Security auditor on Floci")(
    test("unused permissions are posted once, naming their statements, with the model's review in the thread"):
      ZIO.scoped:
        for
          w       <- AgentSpec.world
          _       <- w.pipeline.reconcile(w.envs, 0)
          model   <- ScriptedModel.make.orDie
          bedrock <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          review  <- ReviewFindings.make(bedrock, "review-security").mapError(_.message)
          _       <- model.reply(
                       ScriptedModel.Turn.ToolUse("search", Json.Obj("pattern" -> Json.Str("Bucket\\\\."))),
                       ScriptedModel.Turn.Text("The app only reads `uploads`: declare `Bucket.reader(Uploads)` instead of `readWrite`."),
                     )
          job      = SecurityJob(w.envs.take(1), List(AgentSpec.app), w.slack, w.slack.self, AgentSpec.channel, Some(review), window = 0.days)
          _       <- job.sweep
          _       <- job.sweep
          posted  <- w.slack.posted(AgentSpec.channel).map(_.filter(_.text.startsWith("*Security*")))
          thread  <- w.slack.threadTexts(AgentSpec.channel, posted.head.ts)
                       .repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(100)).map(_._2)
        yield assertTrue(
          posted.size == 1,
          posted.head.text.contains(s"`${AgentSpec.app.value}/${w.envs.head.env.value}`"),
          posted.head.text.contains("`BucketWriteUploads`: s3:DeleteObject, s3:PutObject"),
          posted.head.text.contains("`BucketReadUploads`"),
          thread == List("The app only reads `uploads`: declare `Bucket.reader(Uploads)` instead of `readWrite`."),
        ),
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >>> (AwsEngine.clients ++ ZLayer.environment[Client & AwsConfig])).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
