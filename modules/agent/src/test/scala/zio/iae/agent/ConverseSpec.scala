package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.AwsError
import zio.iae.engine.*
import zio.iae.testkit.{FlociLayer, ScriptedModel}
import zio.json.*
import zio.json.ast.Json
import zio.test.*

/** The `Converse` task against a scripted model: who gets an answer, and the tools it can use. */
object ConverseSpec extends ZIOSpecDefault:
  val channel = AgentSpec.channel

  final case class Setup(w: AgentSpec.World, model: ScriptedModel, converse: Converse)

  val setup: ZIO[Scope & AwsEngine.Clients & Client, String, Setup] =
    for
      w        <- AgentSpec.world
      _        <- w.pipeline.reconcile(w.envs, 0).mapError(_.message)
      model    <- ScriptedModel.make.orDie
      bedrock  <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
      converse <- Converse.make(w.slack, w.slack.self, bedrock, w.envs, CommandManifests()).mapError(_.message)
    yield Setup(w, model, converse)

  def spec = suite("Converse")(
    test("a mention from an approver is answered in its thread, with the status tool's facts given to the model"):
      ZIO.scoped:
        for
          s       <- setup
          _       <- s.model.reply(ScriptedModel.Turn.ToolUse("status", Json.Obj()), ScriptedModel.Turn.Text("Staging runs the latest commit."))
          ts      <- s.w.slack.say(channel, "U_ALICE", s"<@${s.w.slack.self.userId}> what's deployed in staging?")
          _       <- s.converse.handle(InboxEvent.SlackMention(channel, ts, "U_ALICE"))
          answer  <- s.w.slack.threadTexts(channel, ts)
          second  <- s.model.requests.map(_.last.toJson)
          staging  = s.w.envs.head.env.value
        yield assertTrue(
          answer == List("Staging runs the latest commit."),
          second.contains("toolResult"),
          second.contains(staging),
        ),
    test("people outside the policy's groups, threads the agent isn't in, and answered threads get nothing"):
      ZIO.scoped:
        for
          s        <- setup
          carol    <- s.w.slack.say(channel, "U_CAROL", s"<@${s.w.slack.self.userId}> please apply everything")
          _        <- s.converse.handle(InboxEvent.SlackMention(channel, carol, "U_CAROL"))
          chatter  <- s.w.slack.say(channel, "U_ALICE", "lunch?")
          reply    <- s.w.slack.say(channel, "U_BOB", "sure", Some(chatter))
          _        <- s.converse.handle(InboxEvent.SlackReply(channel, chatter, "U_BOB"))
          _        <- s.model.replyText("Hi.")
          asked    <- s.w.slack.say(channel, "U_ALICE", s"<@${s.w.slack.self.userId}> hi")
          _        <- s.converse.handle(InboxEvent.SlackMention(channel, asked, "U_ALICE"))
          _        <- s.converse.handle(InboxEvent.SlackMention(channel, asked, "U_ALICE"))
          calls    <- s.model.requests.map(_.size)
          answered <- s.w.slack.threadTexts(channel, asked)
          ignored  <- s.w.slack.threadTexts(channel, carol)
        yield assertTrue(calls == 1, answered == List("Hi."), ignored.isEmpty),
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >>> (AwsEngine.clients ++ ZLayer.environment[Client])).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
