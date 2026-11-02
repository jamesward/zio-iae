package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*

/** The security auditor's unused-permissions check: on a schedule, each app's runtime role in each env is checked
  * against what it used in the window. Unused permissions are posted once per set (the message names them), with the
  * `ReviewSecurity` task's review in the thread when a model is configured; its pull request, if it opens one, goes
  * through the pipeline and policy like any change.
  */
final case class SecurityJob(
    envs: List[EnvRuntime],
    apps: List[AppName],
    slack: SlackApi,
    self: SlackIdentity,
    channel: String,
    reviewer: Option[FindingsReviewer],
    window: Duration = 90.days,
    every: Duration = 1.day,
):
  def run: UIO[Nothing] = sweep.repeat(Schedule.spaced(every)).forever

  def sweep: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        check(runtime, app).catchAll(e => ZIO.logWarning(s"security check of ${app.value}/${runtime.env.value}: ${e.message}"))

  private def check(runtime: EnvRuntime, app: AppName): IO[AgentError, Unit] =
    for
      now    <- Clock.instant
      unused <- runtime.engine.unusedPermissions(app, runtime.env, now.minus(window).nn).mapError(AgentError.Engine(_))
      _ <- ZIO.when(unused.nonEmpty):
             val marker = SecurityJob.marker(app, runtime.env, unused)
             for
               recent <- slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)
               _ <- ZIO.unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker))):
                      for
                        ts <- slack.post(channel, SecurityJob.text(app, runtime.env, unused, window, marker), None, None)
                        _  <- review(runtime, app, unused, ts)
                      yield ()
             yield ()
    yield ()

  /** In the background: a review informs, it decides nothing. */
  private def review(runtime: EnvRuntime, app: AppName, unused: List[UnusedPermission], ts: String): UIO[Unit] =
    ZIO.foreachDiscard(reviewer): r =>
      (for
        deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
        commit   <- ZIO.fromOption(deployed).orElseFail("nothing deployed")
        text     <- r.review(runtime.workspace, commit, SecurityJob.findings(unused))
        _        <- slack.post(channel, text, Some(ts), None).mapError(_.message)
      yield ()).catchAll(e => ZIO.logWarning(s"reviewing unused permissions of ${app.value}/${runtime.env.value}: $e")).forkDaemon.unit

object SecurityJob:
  def findings(unused: List[UnusedPermission]): String =
    unused.map(u => s"- `${u.statement}`: ${u.actions.mkString(", ")} on ${u.resources.mkString(", ")}").mkString("\n")

  /** Names the set, so the same findings are posted once. */
  def marker(app: AppName, env: EnvName, unused: List[UnusedPermission]): String =
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(unused.map(_.statement).sorted.mkString(",").getBytes(UTF_8))
    s"findings ${HexFormat.of().nn.formatHex(digest).nn.take(12)}"

  def text(app: AppName, env: EnvName, unused: List[UnusedPermission], window: Duration, marker: String): String =
    s"*Security* `${app.value}/${env.value}`: the runtime role hasn't used ${if unused.size == 1 then "this permission" else "these permissions"} " +
      s"in ${window.toDays} days ($marker). Declaring only what the code uses narrows it.\n" + findings(unused)
