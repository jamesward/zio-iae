package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*
import zio.iae.aws.client.InspectorClient
import zio.iae.engine.policy.SlackGroup

/** The security auditor, for each app in each env:
  *   - unused permissions (daily): the runtime role checked against what it used in the window;
  *   - findings (`findingsEvery`, every 15 minutes): the engine's own configuration checks, Security Hub, GuardDuty
  *     and Access Analyzer on the app's resources, and Inspector's on the images it runs (`images`), of at least
  *     medium severity.
  *
  * Each set is posted once (the message names it), with the `ReviewSecurity` task's review in the thread when a
  * model is configured; its pull request, if it opens one, goes through the pipeline and policy like any change.
  * An active threat is escalated at once, mentioning the owners, and never left to a pull request.
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
    owners: Option[SlackGroup] = None,
    images: Option[ImageScans] = None,
    findingsEvery: Duration = 15.minutes,
):
  def run: UIO[Nothing] = (sweep.repeat(Schedule.spaced(every)) <&> findings.repeat(Schedule.spaced(findingsEvery))).as(()).forever

  /** The findings of every app in every env: threats escalated one by one, the rest reported as a set. */
  def findings: UIO[Unit] =
    ZIO.foreachDiscard(envs): runtime =>
      ZIO.foreachDiscard(apps): app =>
        report(runtime, app).catchAll(e => ZIO.logWarning(s"security findings of ${app.value}/${runtime.env.value}: ${e.message}"))

  private def report(runtime: EnvRuntime, app: AppName): IO[AgentError, Unit] =
    for
      found  <- runtime.engine.securityFindings(app, runtime.env).mapError(AgentError.Engine(_))
      scans  <- ZIO.foreach(images)(_.scan(runtime, app)).map(_.getOrElse(Nil))
      all     = (found ++ scans).filter(_.severity.atLeast(Severity.Medium)).sortBy(f => (-f.severity.ordinal, f.source, f.id))
      (threats, rest) = all.partition(_.threat)
      now    <- Clock.instant
      recent <- ZIO.when(all.nonEmpty)(slack.history(channel, now.minusSeconds(30L * 24 * 3600).nn)).map(_.getOrElse(Nil))
      posted  = (marker: String) => recent.exists(m => m.botId.contains(self.botId) && m.text.contains(marker))
      mention <- ZIO.foreach(owners)(o => slack.groupId(o).map(_.fold(s"@${o.handle}")(id => s"<!subteam^$id>")))
      _ <- ZIO.foreachDiscard(threats.filterNot(t => posted(SecurityJob.findingMarker(t)))): t =>
             slack.post(channel, SecurityJob.threat(app, runtime.env, t, mention), None, None)
      marker = SecurityJob.findingsMarker(rest)
      _ <- ZIO.when(rest.nonEmpty && !posted(marker)):
             slack.post(channel, SecurityJob.report(app, runtime.env, rest, marker), None, None).flatMap(review(runtime, app, SecurityJob.list(rest), _))
    yield ()

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
    review(runtime, app, SecurityJob.findings(unused), ts)

  private def review(runtime: EnvRuntime, app: AppName, findings: String, ts: String): UIO[Unit] =
    ZIO.foreachDiscard(reviewer): r =>
      (for
        deployed <- runtime.engine.deployed(List(app), runtime.env).mapError(_.message)
        commit   <- ZIO.fromOption(deployed).orElseFail("nothing deployed")
        text     <- r.review(runtime.workspace, commit, findings)
        _        <- slack.post(channel, text, Some(ts), None).mapError(_.message)
      yield ()).catchAll(e => ZIO.logWarning(s"reviewing unused permissions of ${app.value}/${runtime.env.value}: $e")).forkDaemon.unit

object SecurityJob:
  private def short(id: String): String =
    HexFormat.of().nn.formatHex(MessageDigest.getInstance("SHA-256").nn.digest(id.getBytes(UTF_8))).nn.take(12)

  def findingMarker(f: SecurityFinding): String = s"finding ${short(f.id)}"

  def findingsMarker(found: List[SecurityFinding]): String = s"findings ${short(found.map(_.id).sorted.mkString(","))}"

  private def on(f: SecurityFinding): String = f.resource.fold("")(a => s"${a.kind} `${a.id.value}`: ")

  def list(found: List[SecurityFinding]): String =
    found.map(f => s"- [${f.severity}] ${on(f)}${f.title} (${f.source})${if f.detail.nonEmpty then s" ${f.detail.take(300)}" else ""}").mkString("\n")

  def report(app: AppName, env: EnvName, found: List[SecurityFinding], marker: String): String =
    s"*Security* `${app.value}/${env.value}`: ${found.size} finding(s) ($marker).\n" + list(found)

  def threat(app: AppName, env: EnvName, f: SecurityFinding, mention: Option[String]): String =
    s"*Security threat* `${app.value}/${env.value}`: ${on(f)}${f.title} (${f.source}, ${findingMarker(f)}). " +
      s"${f.detail.take(500)} ${mention.fold("")(_ + " ")}This needs people: iae doesn't change anything for it.".replace("  ", " ")

  def findings(unused: List[UnusedPermission]): String =
    unused.map(u => s"- `${u.statement}`: ${u.actions.mkString(", ")} on ${u.resources.mkString(", ")}").mkString("\n")

  /** Names the set, so the same findings are posted once. */
  def marker(app: AppName, env: EnvName, unused: List[UnusedPermission]): String =
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(unused.map(_.statement).sorted.mkString(",").getBytes(UTF_8))
    s"findings ${HexFormat.of().nn.formatHex(digest).nn.take(12)}"

  def text(app: AppName, env: EnvName, unused: List[UnusedPermission], window: Duration, marker: String): String =
    s"*Security* `${app.value}/${env.value}`: the runtime role hasn't used ${if unused.size == 1 then "this permission" else "these permissions"} " +
      s"in ${window.toDays} days ($marker). Declaring only what the code uses narrows it.\n" + findings(unused)

/** Inspector's findings on the images an app runs in an env (and would roll back to), from the repositories iae keeps
  * for apps whose builds push their image, in the agent's account: high and critical vulnerabilities, fixed by
  * bumping a base image or dependency and rebuilding.
  */
final case class ImageScans(inspector: InspectorClient, apps: Set[AppName]):
  def scan(runtime: EnvRuntime, app: AppName): IO[AgentError, List[SecurityFinding]] =
    if !apps.contains(app) then ZIO.succeed(Nil)
    else
      val repository = CodeBuildManifests.repository(app)
      for
        tags  <- runtime.engine.artifacts(app, runtime.env).mapBoth(AgentError.Engine(_), _.toList.flatMap(ImagePruner.tagOf).sorted)
        found <- inspector.imageFindings(repository, tags).mapError(e => AgentError.Config(s"reading Inspector's findings for $repository: ${e.message}"))
      yield found.flatMap: f =>
        Severity.fromLabel(f.severity).filter(_.atLeast(Severity.High)).map: severity =>
          SecurityFinding("Inspector", f.id, severity, s"${f.title} in image `${f.resources.headOption.getOrElse(tags.mkString(", "))}`", None, f.description.take(300), threat = false)
