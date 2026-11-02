package zio.iae.agent

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.engine.policy.SlackGroup

/** An alarm that went off on a live service. */
final case class Alert(env: EnvName, app: AppName, alarm: WatchedAlarm):
  def where: String = s"${app.value}/${env.value}"

  def headline: String =
    s"the ${alarm.address.id.value} ${alarm.address.kind.toString.toLowerCase}'s ${alarm.gate} alarm (`${alarm.alarm}`) went off" +
      alarm.image.fold("")(i => s", running `$i`") + alarm.commit.fold("")(c => s" from `${c.short}`")

/** Investigates an alert and says what it found. `rollback` asks for the alerting service to go back to its previous
  * version, through the policy, and says what became of it.
  */
trait AlertTriage:
  def triage(alert: Alert, runtime: EnvRuntime, rollback: IO[String, String]): IO[String, String]

/** Asks for a rollback through the pipeline: None if there's nothing to roll back to. */
type Rollbacks = (EnvRuntime, AppName, LogicalId) => IO[AgentError, Option[Outcome]]

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
    rollbacks: Option[Rollbacks] = None,
    pending: Ref[Map[(EnvName, AppName, LogicalId), String]],
):
  import Monitor.*

  def run: UIO[Nothing] =
    (sweep.catchAllCause(c => ZIO.logErrorCause("monitor: the sweep failed", c)).repeat(Schedule.spaced(every)) <&> triaging).as(()).forever

  /** One look at every alarm: opens alerts for the ones firing, resolves the ones that cleared, and asks again for
    * the rollbacks still waiting on approvers.
    */
  def sweep: IO[AgentError, Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        runtime.engine.alarms(app, runtime.env).mapError(e => AgentError.Config(e.message)).flatMap: alarms =>
          ZIO.foreachDiscard(alarms)(a => observe(Alert(runtime.env, app, a), runtime)) *>
            retired(runtime.env, app, alarms.map(_.alarm).toSet)
    *> followUp

  /** An open alert whose alarm no longer watches a live version (a rollback or a deploy moved the traffic away) is
    * over for this env: resolved, saying so.
    */
  private def retired(env: EnvName, app: AppName, watched: Set[String]): IO[AgentError, Unit] =
    val prefix = s"iae-${app.value}-${env.value}-"
    open.updateZIO: known =>
      known.fold(rehydrate)(ZIO.succeed(_)).flatMap: current =>
        val gone = current.filter((alarm, _) => alarm.startsWith(prefix) && !watched.contains(alarm))
        ZIO.foreachDiscard(gone.values)(resolve(_, "its version no longer has the traffic"))
          .as(Some(current -- gone.keys))

  /** A rollback asked for in an alert's thread; while it waits on approvers it is asked again on each sweep. */
  def rollback(alert: Alert, runtime: EnvRuntime, thread: String): IO[String, String] =
    val key = (alert.env, alert.app, alert.alarm.address.id)
    ZIO.fail(s"only a service can be rolled back; this alert is about a ${alert.alarm.address.kind.toString.toLowerCase}")
      .when(alert.alarm.address.kind != ResourceKind.Service) *>
    ZIO.fromOption(rollbacks).orElseFail("rollbacks aren't available").flatMap: ask =>
      ask(runtime, alert.app, alert.alarm.address.id).mapError(_.message).flatMap: outcome =>
        val waiting = outcome.exists { case _: Outcome.WaitingForApproval => true; case _ => false }
        pending.update(p => if waiting then p + (key -> thread) else p - key).as(Monitor.describe(outcome))

  private def followUp: IO[AgentError, Unit] =
    pending.get.flatMap: waiting =>
      ZIO.foreachDiscard(waiting): (key, thread) =>
        val (env, app, service) = key
        ZIO.foreachDiscard(envs.find(_.env == env).zip(rollbacks)): (runtime, ask) =>
          ask(runtime, app, service).flatMap:
            case Some(_: Outcome.WaitingForApproval) => ZIO.unit
            case outcome =>
              pending.update(_ - key) *> slack.post(channel, s"Rollback: ${Monitor.describe(outcome)}.", Some(thread), None).unit

  private def observe(alert: Alert, runtime: EnvRuntime): IO[AgentError, Unit] =
    open.updateZIO: known =>
      known.fold(rehydrate)(ZIO.succeed(_)).flatMap: current =>
        (current.get(alert.alarm.alarm), alert.alarm.firing) match
          case (None, true)         => raise(alert, runtime).map(o => Some(current + (alert.alarm.alarm -> o)))
          case (Some(o), false)     => resolve(o, "the alarm is quiet again") *> pending.update(_ - ((alert.env, alert.app, alert.alarm.address.id))).as(Some(current - alert.alarm.alarm))
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

  private def resolve(opened: Opened, why: String): IO[AgentError, Unit] =
    for
      now <- Clock.instant
      took = java.time.Duration.between(opened.since, now).nn
      _   <- slack.post(channel, s"Cleared: $why${if took.isNegative then "" else s" after ${minutes(took)}"}.", Some(opened.ts), None)
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
          t.triage(alert, runtime, rollback(alert, runtime, ts))
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

  /** What became of a rollback, for the model and the thread. */
  def describe(outcome: Option[Outcome]): String = outcome match
    case None                                      => "there is no previous version to roll back to"
    case Some(Outcome.Converged(_))                => "rolled back to the previous version"
    case Some(Outcome.Submitted(_, plan))          => s"handed to the executor (plan $plan), which checks it again and rolls back"
    case Some(Outcome.WaitingForApproval(_, ts))   => s"the policy needs approvers for it: asked in the channel (message $ts)"
    case Some(Outcome.Stopped(_, why))             => s"not rolled back: $why"
    case Some(Outcome.WaitingForPreviousEnv(env))  => s"waiting for ${env.value}"

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
      rollbacks: Option[Rollbacks] = None,
  ): UIO[Monitor] =
    for
      open    <- Ref.Synchronized.make(Option.empty[Map[String, Opened]])
      work    <- Queue.unbounded[(Alert, EnvRuntime, String)]
      pending <- Ref.make(Map.empty[(EnvName, AppName, LogicalId), String])
    yield Monitor(envs, apps, slack, self, channel, owners, triage, open, work, every, rollbacks, pending)
