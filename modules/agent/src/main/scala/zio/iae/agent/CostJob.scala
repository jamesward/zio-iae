package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*

/** The cost optimizer, on a schedule (weekly), for each app in each env:
  *   - the right-sizing report: the live services checked against Compute Optimizer's recommendations, priced from
  *     the same price book as plans. A set of recommendations is posted once, with the `ReviewCost` task's review in
  *     the thread when a model is configured; a pull request it opens resizes the `ServiceSpec` and goes through the
  *     pipeline and policy like any change;
  *   - the bill: what the app cost over the last 30 days (Cost Explorer by its tags), once a week, beside what the
  *     price model projects for its resources, so a projection that's off is seen.
  *
  * Spending anomalies are checked more often (`anomalyEvery`, hourly) and each is posted once, when it's found.
  */
final case class CostJob(
    envs: List[EnvRuntime],
    apps: List[AppName],
    slack: SlackApi,
    self: SlackIdentity,
    channel: String,
    reviewer: Option[FindingsReviewer],
    every: Duration = 7.days,
    anomalyEvery: Duration = 1.hour,
):
  def run: UIO[Nothing] = (sweep.repeat(Schedule.spaced(every)) <&> anomalies.repeat(Schedule.spaced(anomalyEvery))).as(()).forever

  def sweep: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        check(runtime, app).catchAll(e => ZIO.logWarning(s"cost review of ${app.value}/${runtime.env.value}: ${e.message}")) *>
          bill(runtime, app).catchAll(e => ZIO.logWarning(s"the bill of ${app.value}/${runtime.env.value}: ${e.message}"))

  /** Each anomaly in an env's account from the last two weeks, posted once (envs that share an account share it). */
  def anomalies: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      (for
        today  <- Clock.localDateTime.map(_.toLocalDate.nn)
        found  <- runtime.engine.anomalies(runtime.env, today.minusDays(14).nn).mapError(AgentError.Engine(_))
        now    <- Clock.instant
        recent <- ZIO.when(found.nonEmpty)(slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)).map(_.getOrElse(Nil))
        _ <- ZIO.foreachDiscard(found.filter(_.impact > 0)): a =>
               slack.post(channel, CostJob.anomaly(runtime.env, a), None, None).unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(CostJob.anomalyMarker(a))))
      yield ()).catchAll(e => ZIO.logWarning(s"cost anomalies in ${runtime.env.value}: ${e.message}"))

  /** Once a week (the ISO week names it): the last 30 days' bill, beside the projection. */
  private def bill(runtime: EnvRuntime, app: AppName): IO[AgentError, Unit] =
    for
      today     <- Clock.localDateTime.map(_.toLocalDate.nn)
      spend     <- runtime.engine.spend(app, runtime.env, today.minusDays(30).nn, today).mapError(AgentError.Engine(_))
      projected <- runtime.engine.projected(app, runtime.env).mapError(AgentError.Engine(_))
      marker     = CostJob.weekMarker(app, runtime.env, today)
      now       <- Clock.instant
      _ <- ZIO.foreachDiscard(spend.filter(s => s.total > 0 || projected.exists(_ > 0))): s =>
             slack.history(channel, now.minusSeconds(8L * 24 * 3600).nn).flatMap: recent =>
               slack.post(channel, CostJob.bill(app, runtime.env, s, projected, marker), None, None)
                 .unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker)))
    yield ()

  private def check(runtime: EnvRuntime, app: AppName): IO[AgentError, Unit] =
    (runtime.engine.rightSizing(app, runtime.env) <*> runtime.engine.storage(app, runtime.env)).mapError(AgentError.Engine(_)).flatMap: (found, storage) =>
      ZIO.when(found.nonEmpty || storage.nonEmpty):
        val marker = CostJob.marker(found, storage)
        for
          now    <- Clock.instant
          recent <- slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)
          _ <- ZIO.unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker))):
                 slack.post(channel, CostJob.text(app, runtime.env, found, storage, marker), None, None).flatMap(review(runtime, app, found, storage, _))
        yield ()
      .unit

  private def review(runtime: EnvRuntime, app: AppName, found: List[RightSizing], storage: List[StorageFinding], ts: String): UIO[Unit] =
    ZIO.foreachDiscard(reviewer): r =>
      (for
        deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
        commit   <- ZIO.fromOption(deployed).orElseFail("nothing deployed")
        text     <- r.review(runtime.workspace, commit, CostJob.findings(found, storage))
        _        <- slack.post(channel, text, Some(ts), None).mapError(_.message)
      yield ()).catchAll(e => ZIO.logWarning(s"reviewing the costs of ${app.value}/${runtime.env.value}: $e")).forkDaemon.unit

object CostJob:
  def weekMarker(app: AppName, env: EnvName, day: java.time.LocalDate): String =
    val week = day.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)
    f"bill ${day.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR)}-W$week%02d"

  /** The bill, by service, and how the projection compares: off by more than a quarter (and a dollar) is called out. */
  def bill(app: AppName, env: EnvName, spend: Spend, projected: Option[Double], marker: String): String =
    val services = spend.byService.toList.filter(_._2 >= 0.005).sortBy(-_._2).take(6).map((s, c) => f"$s $$$c%.2f").mkString(", ")
    val compared = projected.fold(" iae has no projection for its resources."): p =>
      val off = if p > 0 then (spend.total - p) / p else 0d
      val note =
        if math.abs(spend.total - p) > 1 && math.abs(off) > 0.25 then
          f" The bill is ${math.abs(off) * 100}%.0f%% ${if off > 0 then "higher" else "lower"} than that: the prices or the usage assumptions plans use may be off."
        else ""
      f" iae projects $$$p%.2f a month for its resources.$note"
    f"*Cost* `${app.value}/${env.value}`: $$${spend.total}%.2f in the last 30 days ($marker)" +
      (if services.nonEmpty then s": $services." else ".") + compared

  def anomalyMarker(a: CostAnomaly): String = s"anomaly ${a.id.split('/').last.take(40)}"

  def anomaly(env: EnvName, a: CostAnomaly): String =
    f"*Cost anomaly* in `${env.value}`'s account: $$${a.impact}%.2f more than expected" +
      a.dimension.fold("")(d => s" on $d") + a.start.fold("")(d => s" since $d") + s" (${anomalyMarker(a)})." +
      (if a.causes.nonEmpty then s" Causes: ${a.causes.take(3).mkString("; ")}." else "")

  def findings(found: List[RightSizing], storage: List[StorageFinding] = Nil): String =
    val services = found.map: r =>
      val saves = if r.monthlySavings >= 0 then f"saves $$${r.monthlySavings}%.2f a month" else f"costs $$${-r.monthlySavings}%.2f a month more"
      s"- ${r.address.kind} `${r.address.id.value}` is ${r.finding.toLowerCase}: ${r.cpu} CPU / ${r.memory} MiB -> ${r.toCpu} CPU / ${r.toMemory} MiB, $saves"
    val stored = storage.map(f => f"- ${f.address.kind} `${f.address.id.value}`: ${f.finding}; ${f.change} saves about $$${f.monthlySavings}%.2f a month")
    (services ++ stored).mkString("\n")

  def marker(found: List[RightSizing], storage: List[StorageFinding] = Nil): String =
    val key    = (found.map(r => s"${r.address.show}:${r.toCpu}/${r.toMemory}") ++ storage.map(f => s"${f.address.show}:${f.change}")).sorted.mkString(",")
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(key.getBytes(UTF_8))
    s"findings ${HexFormat.of().nn.formatHex(digest).nn.take(12)}"

  def text(app: AppName, env: EnvName, found: List[RightSizing], storage: List[StorageFinding], marker: String): String =
    val total = found.map(_.monthlySavings).sum + storage.map(_.monthlySavings).sum
    val what  = (found.nonEmpty, storage.nonEmpty) match
      case (true, true)  => "right-sizing and storing data for less"
      case (true, false) => "right-sizing"
      case _             => "storing data for less"
    f"*Cost* `${app.value}/${env.value}`: $what could save $$${total}%.2f a month ($marker).%n" + findings(found, storage)
