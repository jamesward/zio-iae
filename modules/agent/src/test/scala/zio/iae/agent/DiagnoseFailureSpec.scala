package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.aws.client.AwsError
import zio.iae.engine.*
import zio.iae.testkit.{FlociLayer, ScriptedModel}
import zio.json.*
import zio.json.ast.Json
import zio.test.*

object DiagnoseFailureSpec extends ZIOSpecDefault:
  val channel = AgentSpec.channel

  def spec = suite("DiagnoseFailure")(
    test("a failing build is announced once, and the model's diagnosis, from the files it read, goes in the thread"):
      ZIO.scoped:
        for
          w        <- AgentSpec.world
          model    <- ScriptedModel.make.orDie
          bedrock  <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          diagnose <- DiagnoseFailure.make(bedrock).mapError(_.message)
          pipeline  = w.pipeline.copy(diagnoser = Some(diagnose))
          _        <- model.reply(
                        ScriptedModel.Turn.ToolUse("readFile", Json.Obj("path" -> Json.Str(".iae/manifest.json"))),
                        ScriptedModel.Turn.Text("`.iae/manifest.json` isn't JSON: it ends after `{\"app\"`."),
                      )
          _        <- w.repo.commit(Map(".iae/manifest.json" -> """{"app"""), "break the manifest")
          first    <- pipeline.reconcile(w.envs, 0).either
          notices  <- w.slack.posted(channel).map(_.filter(_.text.startsWith("*Failed*")))
          _        <- w.slack.threadTexts(channel, notices.head.ts).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(50))
          thread   <- w.slack.threadTexts(channel, notices.head.ts)
          second   <- pipeline.reconcile(w.envs, 0).either
          again    <- w.slack.posted(channel).map(_.filter(_.text.startsWith("*Failed*")))
          asked    <- model.requests.map(_.last.toJson)
        yield assertTrue(
          first.isLeft,
          second.isLeft,
          notices.size == 1,
          notices.head.text.contains(s"`${w.envs.head.env.value}`"),
          notices.head.text.contains("building photo-app failed"),
          thread == List("`.iae/manifest.json` isn't JSON: it ends after `{\"app\"`."),
          again.size == 1,
          asked.contains("toolResult"),
          asked.contains(".iae/manifest.json"),
          asked.contains("break") || asked.contains("app"),
        ),
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >>> (AwsEngine.clients ++ ZLayer.environment[Client])).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
