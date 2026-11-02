package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*

/** The cost optimizer's right-sizing report: on a schedule (weekly), each app's live services in each env are checked
  * against Compute Optimizer's recommendations, priced from the same price book as plans. A set of recommendations is
  * posted once, with the `ReviewCost` task's review in the thread when a model is configured; a pull request it
  * opens resizes the `ServiceSpec` and goes through the pipeline and policy like any change.
  */
final case class CostJob(
    envs: List[EnvRuntime],
    apps: List[AppName],
    slack: SlackApi,
    self: SlackIdentity,
    channel: String,
    reviewer: Option[FindingsReviewer],
    every: Duration = 7.days,
):
  def run: UIO[Nothing] = sweep.repeat(Schedule.spaced(every)).forever

  def sweep: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        check(runtime, app).catchAll(e => ZIO.logWarning(s"cost review of ${app.value}/${runtime.env.value}: ${e.message}"))

  private def check(runtime: EnvRuntime, app: AppName): IO[AgentError, Unit] =
    runtime.engine.rightSizing(app, runtime.env).mapError(AgentError.Engine(_)).flatMap: found =>
      ZIO.when(found.nonEmpty):
        val marker = CostJob.marker(found)
        for
          now    <- Clock.instant
          recent <- slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)
          _ <- ZIO.unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker))):
                 slack.post(channel, CostJob.text(app, runtime.env, found, marker), None, None).flatMap(review(runtime, app, found, _))
        yield ()
      .unit

  private def review(runtime: EnvRuntime, app: AppName, found: List[RightSizing], ts: String): UIO[Unit] =
    ZIO.foreachDiscard(reviewer): r =>
      (for
        deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
        commit   <- ZIO.fromOption(deployed).orElseFail("nothing deployed")
        text     <- r.review(runtime.workspace, commit, CostJob.findings(found))
        _        <- slack.post(channel, text, Some(ts), None).mapError(_.message)
      yield ()).catchAll(e => ZIO.logWarning(s"reviewing the costs of ${app.value}/${runtime.env.value}: $e")).forkDaemon.unit

object CostJob:
  def findings(found: List[RightSizing]): String =
    found.map: r =>
      val saves = if r.monthlySavings >= 0 then f"saves $$${r.monthlySavings}%.2f a month" else f"costs $$${-r.monthlySavings}%.2f a month more"
      s"- ${r.address.kind} `${r.address.id.value}` is ${r.finding.toLowerCase}: ${r.cpu} CPU / ${r.memory} MiB -> ${r.toCpu} CPU / ${r.toMemory} MiB, $saves"
    .mkString("\n")

  def marker(found: List[RightSizing]): String =
    val key    = found.map(r => s"${r.address.show}:${r.toCpu}/${r.toMemory}").sorted.mkString(",")
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(key.getBytes(UTF_8))
    s"findings ${HexFormat.of().nn.formatHex(digest).nn.take(12)}"

  def text(app: AppName, env: EnvName, found: List[RightSizing], marker: String): String =
    val total = found.map(_.monthlySavings).sum
    f"*Cost* `${app.value}/${env.value}`: right-sizing could save $$${total}%.2f a month ($marker).%n" + findings(found)
