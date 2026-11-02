package zio.iae.agent

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.engine.policy.SlackGroup

/** An alarm that went off on a live service. */
final case class Alert(env: EnvName, app: AppName, alarm: ServiceAlarm):
  def where: String = s"${app.value}/${env.value}"

  def headline: String =
    s"the ${alarm.address.id.value} service's ${alarm.gate} alarm (`${alarm.alarm}`) went off" +
      alarm.image.fold("")(i => s", running `$i`") + alarm.commit.fold("")(c => s" from `${c.short}`")

/** Investigates an alert and says what it found. */
trait AlertTriage:
  def triage(alert: Alert, runtime: EnvRuntime): IO[String, String]

/** The monitor job: watches the alarms of every app's live services in every env. An alarm going off opens an alert,
  * a message in the channel that mentions the owners, and the triage's findings go in its thread. When the alarm
  * clears, the thread says so and the message is marked resolved. Open alerts are found again in the channel after
  * a restart (the agent's own messages, by the alarm they name), so a restart neither repeats nor loses one.
  */
final case class Monitor(
    envs: List[EnvRuntime],
    apps: List[AppName],
    slack: SlackApi,
    self: SlackIdentity,
    channel: String,
    owners: SlackGroup,
    triage: Option[AlertTriage],
    open: Ref.Synchronized[Option[Map[String, Monitor.Opened]]],
    work: Queue[(Alert, EnvRuntime, String)],
    every: Duration = 1.minute,
):
  import Monitor.*

  def run: UIO[Nothing] =
    (sweep.catchAllCause(c => ZIO.logErrorCause("monitor: the sweep failed", c)).repeat(Schedule.spaced(every)) <&> triaging).as(()).forever

  /** One look at every alarm: opens alerts for the ones firing, resolves the ones that cleared. */
  def sweep: IO[AgentError, Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        runtime.engine.alarms(app, runtime.env).mapError(e => AgentError.Config(e.message)).flatMap: alarms =>
          ZIO.foreachDiscard(alarms)(a => observe(Alert(runtime.env, app, a), runtime))

  private def observe(alert: Alert, runtime: EnvRuntime): IO[AgentError, Unit] =
    open.updateZIO: known =>
      known.fold(rehydrate)(ZIO.succeed(_)).flatMap: current =>
        (current.get(alert.alarm.alarm), alert.alarm.firing) match
          case (None, true)         => raise(alert, runtime).map(o => Some(current + (alert.alarm.alarm -> o)))
          case (Some(o), false)     => resolve(o).as(Some(current - alert.alarm.alarm))
          case _                    => ZIO.succeed(Some(current))

  private def raise(alert: Alert, runtime: EnvRuntime): IO[AgentError, Opened] =
    for
      mention <- slack.groupId(owners).map(_.fold(s"@${owners.handle}")(id => s"<!subteam^$id>"))
      text     = s"$AlertLabel `${alert.where}`: ${alert.headline}. $mention"
      ts      <- slack.post(channel, text, None, None)
      now     <- Clock.instant
      _       <- work.offer((alert, runtime, ts)).when(triage.isDefined)
      _       <- ZIO.logWarning(s"alert in ${alert.where}: ${alert.alarm.alarm}")
    yield Opened(ts, text, now)

  private def resolve(opened: Opened): IO[AgentError, Unit] =
    for
      now <- Clock.instant
      took = java.time.Duration.between(opened.since, now).nn
      _   <- slack.post(channel, s"Cleared: the alarm is quiet again${if took.isNegative then "" else s" after ${minutes(took)}"}.", Some(opened.ts), None)
      _   <- slack.update(channel, opened.ts, opened.text.replaceFirst(java.util.regex.Pattern.quote(AlertLabel), ResolvedLabel), None)
    yield ()

  /** The open alerts the agent posted before (and since) a restart: its own alert messages from the last week. */
  private def rehydrate: IO[AgentError, Map[String, Opened]] =
    for
      now      <- Clock.instant
      messages <- slack.history(channel, now.minusSeconds(7 * 24 * 3600).nn)
    yield messages
      .filter(m => m.botId.contains(self.botId) && m.text.startsWith(AlertLabel))
      .flatMap(m => alarmOf(m.text).map(_ -> Opened(m.ts, m.text, m.postedAt)))
      .toMap

  /** Triage runs one alert at a time, away from the sweep, so a slow model doesn't hold up watching. */
  private def triaging: UIO[Nothing] =
    work.take
      .flatMap: (alert, runtime, ts) =>
        ZIO.foreachDiscard(triage): t =>
          t.triage(alert, runtime)
            .foldZIO(
              e => slack.post(channel, s"Triage didn't finish ($e). <@${self.userId}> can't say more; please take a look.", Some(ts), None),
              notes => slack.post(channel, notes, Some(ts), None),
            )
            .catchAll(e => ZIO.logWarning(s"monitor: posting the triage failed: ${e.message}"))
      .forever

object Monitor:
  val AlertLabel    = "*Alert*"
  val ResolvedLabel = "*Resolved*"

  final case class Opened(ts: String, text: String, since: Instant)

  private val alarmName = "\\(`(iae-[^`]+)`\\)".r

  def alarmOf(text: String): Option[String] = alarmName.findFirstMatchIn(text).map(_.group(1).nn)

  private def minutes(d: java.time.Duration): String =
    val m = d.toMinutes
    if m < 1 then "less than a minute" else if m == 1 then "1 minute" else s"$m minutes"

  def make(
      envs: List[EnvRuntime],
      apps: List[AppName],
      slack: SlackApi,
      self: SlackIdentity,
      channel: String,
      owners: SlackGroup,
      triage: Option[AlertTriage],
      every: Duration = 1.minute,
  ): UIO[Monitor] =
    for
      open <- Ref.Synchronized.make(Option.empty[Map[String, Opened]])
      work <- Queue.unbounded[(Alert, EnvRuntime, String)]
    yield Monitor(envs, apps, slack, self, channel, owners, triage, open, work, every)
