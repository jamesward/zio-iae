package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*
import zio.iae.aws.client.{EcrClient, EcrImage}

/** The cleanup job, on a schedule (hourly), in each env:
  *   - for each app the config declares, and each app found in the env that it doesn't declare any more, proposes
  *     the app's cleanup through the policy (`origin = Cleanup`, `Pipeline.cleanup`). It runs often: a proposal
  *     waiting on approvers is found again (the open request), and is applied on the run after it's approved;
  *   - reports the declared apps' idle resources (`IdleReport`), which only a change to the code removes;
  *   - prunes old images of the apps whose builds push them (`ImagePruner`).
  */
final case class CleanupJob(
    envs: List[EnvRuntime],
    apps: List[AppName],
    cleanup: (EnvRuntime, AppName) => IO[AgentError, Option[Outcome]],
    every: Duration = 1.hour,
    idle: Option[IdleReport] = None,
    images: Option[ImagePruner] = None,
):
  def run: UIO[Nothing] =
    sweep.repeat(Schedule.spaced(every)).forever

  def sweep: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      for
        found <- runtime.engine.apps(runtime.env).catchAll(e => ZIO.logWarning(s"finding the apps in ${runtime.env.value}: ${e.message}").as(Set.empty[AppName]))
        _ <- ZIO.foreachDiscard((apps ++ found.toList.sortBy(_.value)).distinct): app =>
               cleanup(runtime, app).catchAll(e => ZIO.logWarning(s"cleanup of ${app.value}/${runtime.env.value}: ${e.message}"))
        _ <- ZIO.foreachDiscard(idle)(r => ZIO.foreachDiscard(apps)(app => r.check(runtime, app)))
      yield ()
    *> ZIO.foreachDiscard(images)(_.sweep(envs))

/** Resources nothing has used for the window (no messages, requests, reads or writes), posted once per set with the
  * `ReviewCleanup` task's review in the thread when a model is configured. Removing one is a change to the `Infra`
  * declaration, which its pull request, if it opens one, makes; that then goes through the pipeline and policy.
  */
final case class IdleReport(slack: SlackApi, self: SlackIdentity, channel: String, reviewer: Option[FindingsReviewer], window: java.time.Duration = Cleaning.usageWindow):
  def check(runtime: EnvRuntime, app: AppName): UIO[Unit] =
    (for
      now   <- Clock.instant
      usage <- runtime.engine.usage(app, runtime.env, now.minus(window).nn).mapError(AgentError.Engine(_))
      idle   = usage.filter(_.idle)
      _ <- ZIO.when(idle.nonEmpty):
             val marker = IdleReport.marker(idle)
             for
               recent <- slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)
               _ <- ZIO.unless(recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker))):
                      slack.post(channel, IdleReport.text(app, runtime.env, idle, window, marker), None, None).flatMap(review(runtime, app, idle, _))
             yield ()
    yield ()).catchAll(e => ZIO.logWarning(s"idle check of ${app.value}/${runtime.env.value}: ${e.message}"))

  private def review(runtime: EnvRuntime, app: AppName, idle: List[ResourceUsage], ts: String): UIO[Unit] =
    ZIO.foreachDiscard(reviewer): r =>
      (for
        deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
        commit   <- ZIO.fromOption(deployed).orElseFail("nothing deployed")
        text     <- r.review(runtime.workspace, commit, IdleReport.findings(idle))
        _        <- slack.post(channel, text, Some(ts), None).mapError(_.message)
      yield ()).catchAll(e => ZIO.logWarning(s"reviewing idle resources of ${app.value}/${runtime.env.value}: $e")).forkDaemon.unit

object IdleReport:
  def findings(idle: List[ResourceUsage]): String =
    idle.map(u => s"- ${u.address.kind} `${u.address.id.value}`: ${u.summary}").mkString("\n")

  def marker(idle: List[ResourceUsage]): String =
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(idle.map(_.address.show).sorted.mkString(",").getBytes(UTF_8))
    s"findings ${HexFormat.of().nn.formatHex(digest).nn.take(12)}"

  def text(app: AppName, env: EnvName, idle: List[ResourceUsage], window: java.time.Duration, marker: String): String =
    s"*Cleanup* `${app.value}/${env.value}`: nothing has used ${if idle.size == 1 then "this resource" else "these resources"} in ${window.toDays} days ($marker). " +
      "If they're no longer needed, removing them from the `Infra` declaration orphans them.\n" + findings(idle)

/** Old images in the repositories iae keeps for apps whose builds push their image (`iae/<app>` in the agent's
  * account). Images are the builds' output, which the agent itself pushes, not an env's infrastructure, so pruning
  * them is the agent's own housekeeping: no plan or policy. It deletes an image only when:
  *   - no env runs it or would roll back to it (what the engine's artifacts say for every env);
  *   - it isn't one of the newest `keep`;
  *   - it was pushed before the oldest image in use, so a build waiting to be deployed is never touched.
  *
  * With nothing deployed anywhere it deletes nothing. What it deleted is posted to the channel.
  */
final case class ImagePruner(ecr: EcrClient, apps: List[AppName], keep: Int, slack: SlackApi, channel: String):
  def sweep(envs: List[EnvRuntime]): UIO[Unit] =
    ZIO.foreachDiscard(apps): app =>
      prune(app, envs)
        .flatMap(deleted => ZIO.when(deleted.nonEmpty)(slack.post(channel, ImagePruner.text(app, deleted, keep), None, None)))
        .catchAll(e => ZIO.logWarning(s"pruning images of ${app.value}: ${e.message}"))

  /** The images deleted. */
  def prune(app: AppName, envs: List[EnvRuntime]): IO[AgentError, List[EcrImage]] =
    val repository = CodeBuildManifests.repository(app)
    for
      inUse  <- ZIO.foreach(envs)(r => r.engine.artifacts(app, r.env)).mapBoth(AgentError.Engine(_), _.flatten.flatMap(ImagePruner.tagOf).toSet)
      images <- ecr.images(repository).mapError(e => AgentError.Config(s"listing $repository: ${e.message}"))
      doomed  = ImagePruner.doomed(images, inUse, keep)
      _      <- ZIO.when(doomed.nonEmpty)(ecr.deleteImages(repository, doomed.map(_.digest)).mapError(e => AgentError.Config(s"deleting from $repository: ${e.message}")))
      _      <- ZIO.logInfo(s"pruned ${doomed.size} image(s) of ${app.value}").when(doomed.nonEmpty)
    yield doomed

object ImagePruner:
  /** The tag of an image reference (`registry/repo:tag`), or the digest of one by digest (`registry/repo@sha256:…`). */
  def tagOf(image: String): Option[String] =
    val last = image.split('/').last
    if last.contains('@') then Some(last.substring(last.indexOf('@') + 1))
    else Option.when(last.contains(':'))(last.substring(last.lastIndexOf(':') + 1))

  def doomed(images: List[EcrImage], inUse: Set[String], keep: Int): List[EcrImage] =
    def used(i: EcrImage) = i.tags.exists(inUse.contains) || inUse.contains(i.digest)
    val oldestInUse = images.filter(used).flatMap(_.pushedAt).minOption
    oldestInUse.toList.flatMap: oldest =>
      images
        .sortBy(_.pushedAt.map(-_.toEpochMilli).getOrElse(0L))
        .drop(keep)
        .filter(i => !used(i) && i.pushedAt.exists(_.isBefore(oldest)))

  def text(app: AppName, deleted: List[EcrImage], keep: Int): String =
    s"*Cleanup* `${app.value}`: deleted ${deleted.size} old image(s) from `${CodeBuildManifests.repository(app)}` that no env runs or rolls back to " +
      s"(the newest $keep and everything since the oldest in use are kept): " + deleted.flatMap(_.tags.headOption).take(20).map(t => s"`$t`").mkString(", ")
