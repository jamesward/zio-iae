package zio.iae.agent

import zio.*
import zio.iae.*

/** The cleanup job: on a schedule, proposes deleting each app's resources that have been orphaned longer than the
  * retention, through the policy (`origin = Cleanup`). It runs often, not just daily: a proposal waiting on approvers
  * is found again (the open request), and is applied on the run after it's approved.
  */
final case class CleanupJob(envs: List[EnvRuntime], apps: List[AppName], cleanup: (EnvRuntime, AppName) => IO[AgentError, Option[Outcome]], every: Duration = 1.hour):
  def run: UIO[Nothing] =
    sweep.repeat(Schedule.spaced(every)).forever

  def sweep: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        cleanup(runtime, app).catchAll(e => ZIO.logWarning(s"cleanup of ${app.value}/${runtime.env.value}: ${e.message}"))
