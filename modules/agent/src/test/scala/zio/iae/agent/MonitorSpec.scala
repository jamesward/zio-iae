package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.SlackGroup
import zio.iae.testkit.ScriptedModel
import zio.json.*
import zio.json.ast.Json
import zio.test.*

/** The monitor over a scripted watch: an alert opens once, gets the triage in its thread, and resolves when the alarm
  * clears, also across a restart.
  */
object MonitorSpec extends ZIOSpecDefault:
  val app     = AppName("photo-app")
  val env     = EnvName("prod")
  val channel = "C_ALERTS"
  val owners  = SlackGroup.from("platform-owners").toOption.get
  val address = Address(app, env, ResourceKind.Service, LogicalId("web"))

  def alarm(firing: Boolean) =
    WatchedAlarm(address, "iae-photo-app-prod-web-green-5xx", "5xx", firing, Some("web:v2"), None)

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  /** An engine that manages nothing and watches whatever the ref says. */
  def runtime(alarms: Ref[List[WatchedAlarm]], workspace: GitWorkspace): EnvRuntime =
    val watch: Watch    = (_, _) => alarms.get
    val none: Discovery = (_, _) => ZIO.succeed(Nil)
    EnvRuntime(env, Engine.make(Drivers(Nil), none, AnyOrder, watch), workspace)

  def spec = suite("Monitor")(
    test("a firing alarm opens one alert with the triage in its thread; it resolves when the alarm clears"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          _       <- repo.commit(Map("README.md" -> "web"), "v2: answer faster")
          ws       = repo.workspace(env)
          _       <- ws.sync.mapError(_.message)
          alarms  <- Ref.make(List(alarm(firing = false)))
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          model   <- ScriptedModel.make.orDie
          bedrock <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          triage  <- Triage.make(bedrock).mapError(_.message)
          _       <- model.reply(
                       ScriptedModel.Turn.ToolUse("deployment", Json.Obj()),
                       ScriptedModel.Turn.Text("The 5xx started with `web:v2`, deployed a minute before. I recommend rolling back."),
                     )
          monitor <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, Some(triage))
          worker  <- monitor.run.fork
          _       <- monitor.sweep.mapError(_.message)
          _       <- alarms.set(List(alarm(firing = true)))
          _       <- monitor.sweep.mapError(_.message)
          _       <- monitor.sweep.mapError(_.message)
          alerts  <- slack.posted(channel)
          ts       = alerts.head.ts
          notes   <- slack.threadTexts(channel, ts).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(50)).map(_._2)
          asked   <- model.requests.map(_.last.toJson)
          _       <- alarms.set(List(alarm(firing = false)))
          _       <- monitor.sweep.mapError(_.message)
          after   <- slack.posted(channel)
          thread  <- slack.threadTexts(channel, ts)
          _       <- worker.interrupt
        yield assertTrue(
          alerts.size == 1,
          alerts.head.text.startsWith("*Alert* `photo-app/prod`: the web service's 5xx alarm (`iae-photo-app-prod-web-green-5xx`) went off"),
          alerts.head.text.contains("`web:v2`"),
          alerts.head.text.contains("<!subteam^S_platform-owners>"),
          notes == List("The 5xx started with `web:v2`, deployed a minute before. I recommend rolling back."),
          asked.contains("v2: answer faster"),
          after.size == 1,
          after.head.text.startsWith("*Resolved*"),
          thread.last.startsWith("Cleared: the alarm is quiet again"),
        ),
    test("the triage's rollback goes through the pipeline; one waiting on approvers is asked again until it's done"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          _       <- repo.commit(Map("README.md" -> "web"), "v2")
          ws       = repo.workspace(env)
          _       <- ws.sync.mapError(_.message)
          alarms  <- Ref.make(List(alarm(firing = true)))
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          model   <- ScriptedModel.make.orDie
          bedrock <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          triage  <- Triage.make(bedrock).mapError(_.message)
          _       <- model.reply(
                       ScriptedModel.Turn.ToolUse("rollback", Json.Obj()),
                       ScriptedModel.Turn.Text("v2 broke it; I asked to roll back, and the policy wants an approver."),
                     )
          asked   <- Ref.make(0)
          rollbacks: Rollbacks = (_, _, service) =>
                       asked.updateAndGet(_ + 1).map: n =>
                         if n == 1 then Some(Outcome.WaitingForApproval(app, "1700000000.000001")) else Some(Outcome.Submitted(app, s"rollback-${service.value}"))
          monitor <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, Some(triage), rollbacks = Some(rollbacks))
          worker  <- monitor.run.fork
          alert   <- slack.posted(channel).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[SlackMessage]](_.nonEmpty) && Schedule.recurs(50)).map(_._2.head)
          notes   <- slack.threadTexts(channel, alert.ts).repeat(Schedule.spaced(100.millis) && Schedule.recurUntil[List[String]](_.nonEmpty) && Schedule.recurs(50)).map(_._2)
          result  <- model.requests.map(_.last.toJson)
          _       <- monitor.sweep.mapError(_.message)
          thread  <- slack.threadTexts(channel, alert.ts)
          calls   <- asked.get
          _       <- monitor.sweep.mapError(_.message)
          again   <- asked.get
          _       <- worker.interrupt
        yield assertTrue(
          notes == List("v2 broke it; I asked to roll back, and the policy wants an approver."),
          result.contains("the policy needs approvers for it"),
          calls == 2,
          thread.last == "Rollback: handed to the executor (plan rollback-web), which checks it again and rolls back.",
          again == 2,
        ),
    test("a degraded service is raised once it lasts two sweeps, and resolved when healthy again"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          ws       = repo.workspace(env)
          health   = (firing: Boolean) =>
                       List(WatchedAlarm(address, "iae-photo-app-prod-web-green-health", if firing then "health (1 of 2 tasks running)" else "health", firing, Some("web:v2"), None))
          alarms  <- Ref.make(health(true))
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          monitor <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, None)
          _       <- monitor.sweep.mapError(_.message)
          once    <- slack.posted(channel)
          _       <- monitor.sweep.mapError(_.message)
          twice   <- slack.posted(channel)
          _       <- alarms.set(health(false))
          _       <- monitor.sweep.mapError(_.message)
          after   <- slack.posted(channel)
          thread  <- slack.threadTexts(channel, after.head.ts)
        yield assertTrue(
          once.isEmpty,
          twice.size == 1,
          twice.head.text.contains("the web service is degraded without an alarm: 1 of 2 tasks running"),
          after.head.text.startsWith("*Resolved*"),
          thread.last.startsWith("Cleared: healthy again"),
        ),
    test("an alert whose version loses the traffic (a rollback) is resolved, saying so"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          ws       = repo.workspace(env)
          alarms  <- Ref.make(List(alarm(firing = true)))
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          monitor <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, None)
          _       <- monitor.sweep.mapError(_.message)
          // Rolled back: blue is live, and its alarms are quiet.
          _       <- alarms.set(List(alarm(firing = false).copy(alarm = "iae-photo-app-prod-web-blue-5xx")))
          _       <- monitor.sweep.mapError(_.message)
          after   <- slack.posted(channel)
          thread  <- slack.threadTexts(channel, after.head.ts)
        yield assertTrue(after.size == 1, after.head.text.startsWith("*Resolved*"), thread.last.startsWith("Cleared: its version no longer has the traffic")),
    test("after a restart, an open alert is found in the channel: not raised again, and resolved when it clears"):
      ZIO.scoped:
        for
          repo   <- TestRepo.make
          ws      = repo.workspace(env)
          alarms <- Ref.make(List(alarm(firing = true)))
          slack  <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          first  <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, None)
          _      <- first.sweep.mapError(_.message)
          // A new process: nothing in memory.
          second <- Monitor.make(List(runtime(alarms, ws)), List(app), slack, slack.self, channel, owners, None)
          _      <- second.sweep.mapError(_.message)
          still  <- slack.posted(channel)
          _      <- alarms.set(List(alarm(firing = false)))
          _      <- second.sweep.mapError(_.message)
          after  <- slack.posted(channel)
        yield assertTrue(still.size == 1, after.size == 1, after.head.text.startsWith("*Resolved*")),
  ).provideShared(Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
