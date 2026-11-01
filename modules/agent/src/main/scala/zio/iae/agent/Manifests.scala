package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.aws.client.CodeBuildClient.*
import zio.stream.ZSink

/** Produces the manifests of a checked-out commit. */
trait ManifestSource:
  def manifests(dir: Path, config: RepoConfig, commit: CommitSha): IO[AgentError, List[Manifest]]

object ManifestSource:
  def parse(app: AppEntry, out: String): IO[AgentError, Manifest] =
    for
      manifest <- ZIO.fromEither(Manifest.fromJson(out)).mapError(AgentError.Manifest(app.name.value, _))
      _        <- ZIO.fail(AgentError.Manifest(app.name.value, s"the command printed the manifest of ${manifest.app.value}")).when(manifest.app != app.name)
    yield manifest

/** Runs each app's build and manifest commands in the agent's own workspace. Only for development and tests: it runs
  * the repo's code inside the agent, with the agent's credentials. Production builds run in CodeBuild.
  */
final case class CommandManifests(timeout: Duration = 10.minutes) extends ManifestSource:
  def manifests(dir: Path, config: RepoConfig, commit: CommitSha): IO[AgentError, List[Manifest]] =
    ZIO.foreach(config.apps): app =>
      for
        _   <- ZIO.fail(AgentError.Manifest(app.name.value, "no manifest command")).when(app.manifest.isEmpty)
        _   <- ZIO.foreachDiscard(app.build)(c => Process.run(dir, Seq("sh", "-c", c), timeout).mapError(AgentError.Manifest(app.name.value, _)))
        out <- Process.run(dir, app.manifest, timeout).mapError(AgentError.Manifest(app.name.value, _))
        m   <- ManifestSource.parse(app, out)
      yield m

/** Builds each app in CodeBuild, from the commit's source zipped into the build bucket, and reads back the manifest
  * its manifest command printed. Each build gets a project of its own, deleted when it's done, so nothing about a build
  * depends on per-build overrides. A commit's manifest is kept in the bucket (`iae/manifests/<app>/<commit>.json`),
  * so each commit is built once however many envs and reconciles ask, including after a restart. The service role
  * should reach only the build bucket: builds run the repo's code and must not reach the env accounts.
  */
final case class CodeBuildManifests(
    codebuild: CodeBuildClient,
    s3: S3Client,
    bucket: BucketName,
    serviceRole: String,
    image: String,
    inflight: Ref.Synchronized[Map[String, Promise[AgentError, Manifest]]],
    timeout: Duration = 30.minutes,
) extends ManifestSource:
  def manifests(dir: Path, config: RepoConfig, commit: CommitSha): IO[AgentError, List[Manifest]] =
    ZIO.foreach(config.apps)(app => cached(app, commit).someOrElseZIO(once(dir, app, commit)))

  /** Envs asking for the same commit at once share one build. */
  private def once(dir: Path, app: AppEntry, commit: CommitSha): IO[AgentError, Manifest] =
    val id = s"${app.name.value}@${commit.value}"
    Promise.make[AgentError, Manifest].flatMap: mine =>
      inflight
        .modify: m =>
          m.get(id) match
            case Some(running) => ((false, running), m)
            case None          => ((true, mine), m + (id -> mine))
        .flatMap: (leader, promise) =>
          if !leader then promise.await
          else build(dir, app, commit).onExit(e => promise.done(e) *> inflight.update(_ - id))

  private def fail(app: AppEntry)(detail: String) = AgentError.Manifest(app.name.value, detail)

  private def key(value: String): S3Key = S3Key.from(value).toOption.get

  private def manifestKey(app: AppEntry, commit: CommitSha) = key(s"iae/manifests/${app.name.value}/${commit.value}.json")

  private def read(k: S3Key): IO[S3Error, Option[String]] =
    s3.headObject(bucket, k).flatMap:
      case None    => ZIO.none
      case Some(_) => s3.getObject(bucket, k).run(ZSink.collectAll[Byte]).map(b => Some(String(b.toArray, UTF_8)))

  private def cached(app: AppEntry, commit: CommitSha): IO[AgentError, Option[Manifest]] =
    read(manifestKey(app, commit)).mapError(e => fail(app)(s"reading the cached manifest: $e")).flatMap(ZIO.foreach(_)(ManifestSource.parse(app, _)))

  private def build(dir: Path, app: AppEntry, commit: CommitSha): IO[AgentError, Manifest] =
    val token  = s"${app.name.value}-${commit.short}-${java.util.UUID.randomUUID.toString.take(8)}"
    val name   = s"iae-build-$token"
    val output = s"iae-manifest-$token.json"
    val source = key(s"iae/sources/${commit.value}.zip")
    val project = CreateProject(
      name,
      Source("S3", Some(s"${bucket.value}/${source.value}"), Some(CodeBuildManifests.buildspec(app, output))),
      Artifacts("S3", Some(bucket.value), Some(s"iae/builds/$token"), Some("out"), Some("NONE"), Some("NONE")),
      Environment("LINUX_CONTAINER", app.image.getOrElse(image), "BUILD_GENERAL1_SMALL",
        Some(List(EnvironmentVariable("IAE_COMMIT", commit.value), EnvironmentVariable("IAE_APP", app.name.value)))),
      serviceRole,
      timeout.toMinutes.toInt.max(5),
      List(CodeBuildClient.Tag("iae:managed", "true"), CodeBuildClient.Tag("iae:app", app.name.value), CodeBuildClient.Tag("iae:commit", commit.value)),
    )
    ZIO.scoped:
      for
        _  <- upload(dir, app, commit, source)
        _  <- ZIO.logInfo(s"building ${app.name.value} at ${commit.short} in CodeBuild")
        _  <- ZIO.acquireRelease(codebuild.createProject(project).mapError(e => fail(app)(s"creating the build project: ${e.message}")))(_ =>
                codebuild.deleteProject(name).ignoreLogged)
        id <- codebuild.startBuild(name).mapError(e => fail(app)(s"starting the build: ${e.message}"))
        done <- finished(app, id)
        _  <- ZIO.fail(fail(app)(s"build ${id.value} ${done.buildStatus.toLowerCase}: ${done.failures.mkString("; ")}")).unless(done.buildStatus == "SUCCEEDED")
        // Where CodeBuild puts the artifact, or the bucket's root where Floci does.
        out <- read(key(s"iae/builds/$token/out/$output")).someOrElseZIO(read(key(output)).someOrFail(S3Error.NoSuchKey(bucket, key(output))))
                 .mapError(e => fail(app)(s"build ${id.value} left no manifest: $e"))
        manifest <- ManifestSource.parse(app, out)
        _        <- s3.putObject(bucket, manifestKey(app, commit), Chunk.fromArray(out.getBytes(UTF_8)), Some("application/json")).mapError(e => fail(app)(e.toString))
      yield manifest

  private def upload(dir: Path, app: AppEntry, commit: CommitSha, source: S3Key): IO[AgentError, Unit] =
    s3.headObject(bucket, source).mapError(e => fail(app)(e.toString)).flatMap:
      case Some(_) => ZIO.unit
      case None =>
        ZIO.scoped:
          for
            zip   <- ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempFile("iae-source", ".zip").nn))(f => ZIO.attemptBlocking(Files.deleteIfExists(f)).ignore)
                       .mapError(e => fail(app)(e.getMessage))
            _     <- GitWorkspace.run(dir, "archive", "--format=zip", "-o", zip.toString, commit.value)
            bytes <- ZIO.attemptBlocking(Files.readAllBytes(zip).nn).mapError(e => fail(app)(e.getMessage))
            _     <- s3.putObject(bucket, source, Chunk.fromArray(bytes), Some("application/zip")).mapError(e => fail(app)(e.toString))
          yield ()

  /** Polls in real time, whatever the agent's clock. */
  private def finished(app: AppEntry, id: BuildId): IO[AgentError, Build] =
    codebuild.build(id)
      .mapError(e => fail(app)(s"reading build ${id.value}: ${e.message}"))
      .repeat(Schedule.spaced(2.seconds) && Schedule.recurUntil[Option[Build]](_.exists(_.buildComplete)))
      .map(_._2)
      .someOrFail(fail(app)(s"build ${id.value} disappeared"))
      .timeoutFail(fail(app)(s"build ${id.value} didn't finish within ${timeout.render}"))(timeout + 1.minute)
      .withClock(Clock.ClockLive)

object CodeBuildManifests:
  /** Creates the build bucket if it's missing. */
  def make(codebuild: CodeBuildClient, s3: S3Client, bucket: BucketName, serviceRole: String, image: String): IO[AgentError, CodeBuildManifests] =
    for
      exists <- s3.bucketExists(bucket).mapError(e => AgentError.Config(s"build bucket: $e"))
      _      <- s3.createBucket(bucket).mapError(e => AgentError.Config(s"creating the build bucket: $e")).unless(exists)
      inflight <- Ref.Synchronized.make(Map.empty[String, Promise[AgentError, Manifest]])
    yield CodeBuildManifests(codebuild, s3, bucket, serviceRole, image, inflight)

  /** The app's build commands, then its manifest command with its output kept as the build's artifact. */
  def buildspec(app: AppEntry, output: String): String =
    val manifest = app.manifest.map(quote).mkString(" ")
    val commands = (app.build :+ s"$manifest > $output").map(c => s"      - ${yamlString(c)}")
    s"""version: 0.2
       |phases:
       |  build:
       |    commands:
       |${commands.mkString("\n")}
       |artifacts:
       |  files:
       |    - $output
       |""".stripMargin

  private def quote(arg: String): String = "'" + arg.replace("'", "'\"'\"'") + "'"

  private def yamlString(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
