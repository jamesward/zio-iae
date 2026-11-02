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
  /** The manifest the app printed, with its services' image set to the commit's (when the app has one). */
  def parse(app: AppEntry, out: String, image: Option[String]): IO[AgentError, Manifest] =
    for
      manifest <- ZIO.fromEither(Manifest.fromJson(out)).mapError(AgentError.Manifest(app.name.value, _))
      _        <- ZIO.fail(AgentError.Manifest(app.name.value, s"the command printed the manifest of ${manifest.app.value}")).when(manifest.app != app.name)
    yield withImage(manifest, image)

  /** The image of an app that names its own (`container`), at the commit. */
  def container(app: AppEntry, commit: CommitSha): Option[String] = app.container.map(_.replace("${commit}", commit.value))

  def withImage(manifest: Manifest, image: Option[String]): Manifest =
    image.fold(manifest): i =>
      manifest.copy(resources = manifest.resources.map:
        case ResourceDecl(id, ResourceSpec.Service(spec)) => ResourceDecl(id, ResourceSpec.Service(spec.copy(image = Some(i))))
        case other                                        => other
      )

/** Runs each app's build and manifest commands in the agent's own workspace. Only for development and tests: it runs
  * the repo's code inside the agent, with the agent's credentials. Production builds run in CodeBuild.
  */
final case class CommandManifests(timeout: Duration = 10.minutes) extends ManifestSource:
  def manifests(dir: Path, config: RepoConfig, commit: CommitSha): IO[AgentError, List[Manifest]] =
    ZIO.foreach(config.apps): app =>
      for
        _   <- ZIO.fail(AgentError.Manifest(app.name.value, "no manifest command")).when(app.manifest.isEmpty)
        _   <- ZIO.fail(AgentError.Manifest(app.name.value, "an app whose build pushes its image needs CodeBuild builds (IAE_BUILD_BUCKET)")).when(app.pushesImage)
        _   <- ZIO.foreachDiscard(app.build)(c => Process.run(dir, Seq("sh", "-c", c), timeout).mapError(AgentError.Manifest(app.name.value, _)))
        out <- Process.run(dir, app.manifest, timeout).mapError(AgentError.Manifest(app.name.value, _))
        m   <- ManifestSource.parse(app, out, ManifestSource.container(app, commit))
      yield m

/** Builds each app in CodeBuild, from the commit's source zipped into the build bucket, and reads back the manifest
  * its manifest command printed. Each build gets a project of its own, deleted when it's done, so nothing about a build
  * depends on per-build overrides. A commit's manifest is kept in the bucket (`iae/manifests/<app>/<commit>.json`),
  * so each commit is built once however many envs and reconciles ask, including after a restart. The service role
  * should reach only the build bucket (and, for apps whose build pushes their image, their repositories): builds run
  * the repo's code and must not reach the env accounts.
  *
  * An app with `pushesImage` gets an ECR repository, `iae/<app>` in the builds' account, which the env accounts in
  * the repo's config may pull from. Its build is given `IAE_IMAGE` (`<repository>:<commit>`) to push, and fails if it
  * doesn't; its services run that image.
  */
final case class CodeBuildManifests(
    codebuild: CodeBuildClient,
    s3: S3Client,
    bucket: BucketName,
    serviceRole: String,
    image: String,
    registry: Option[EcrClient],
    repositories: Ref.Synchronized[Map[(AppName, Set[String]), String]],
    inflight: Ref.Synchronized[Map[String, Promise[AgentError, Manifest]]],
    failures: Ref[Map[String, (AgentError, java.time.Instant)]],
    timeout: Duration = 30.minutes,
    retryFailedAfter: Duration = 30.minutes,
) extends ManifestSource:
  def manifests(dir: Path, config: RepoConfig, commit: CommitSha): IO[AgentError, List[Manifest]] =
    ZIO.foreach(config.apps): app =>
      imageOf(app, config, commit).flatMap(image => cached(app, commit, image).someOrElseZIO(once(dir, app, commit, image)))

  /** The app's image at the commit: its own (`container`), or the one its build pushes to the repository iae keeps. */
  private def imageOf(app: AppEntry, config: RepoConfig, commit: CommitSha): IO[AgentError, Option[String]] =
    if !app.pushesImage then ZIO.succeed(ManifestSource.container(app, commit))
    else
      for
        ecr        <- ZIO.fromOption(registry).orElseFail(fail(app)("its build pushes an image, but the agent has no image registry"))
        _          <- ZIO.fail(fail(app)("an app's image is either its own (container) or pushed by its build (pushesImage), not both")).when(app.container.isDefined)
        pullers     = config.accounts.values.flatMap(a => CodeBuildManifests.accountOf(a.deployer)).toSet
        repository <- repositories.modifyZIO: known =>
                        known.get((app.name, pullers)) match
                          case Some(uri) => ZIO.succeed((uri, known))
                          case None =>
                            val name = CodeBuildManifests.repository(app.name)
                            (for
                              uri <- ecr.ensureRepository(name, Map("iae:managed" -> "true", "iae:app" -> app.name.value))
                              _   <- ecr.setPullers(name, pullers).when(pullers.nonEmpty)
                            yield (uri, known + ((app.name, pullers) -> uri))).mapError(e => fail(app)(s"the image repository: ${e.message}"))
      yield Some(s"$repository:${commit.value}")

  /** Envs asking for the same commit at once share one build, and a commit whose build failed isn't built again for
    * a while (every reconcile would otherwise start a build); its failure is repeated instead.
    */
  private def once(dir: Path, app: AppEntry, commit: CommitSha, image: Option[String]): IO[AgentError, Manifest] =
    val id = s"${app.name.value}@${commit.value}"
    for
      now    <- Clock.instant
      failed <- failures.get.map(_.get(id).filter(_._2.plus(retryFailedAfter).nn.isAfter(now)))
      result <- failed.fold(shared(dir, app, commit, id, image))((e, _) => ZIO.fail(e))
    yield result

  private def shared(dir: Path, app: AppEntry, commit: CommitSha, id: String, image: Option[String]): IO[AgentError, Manifest] =
    Promise.make[AgentError, Manifest].flatMap: mine =>
      inflight
        .modify: m =>
          m.get(id) match
            case Some(running) => ((false, running), m)
            case None          => ((true, mine), m + (id -> mine))
        .flatMap: (leader, promise) =>
          if !leader then promise.await
          else
            build(dir, app, commit, image)
              .tapError(e => Clock.instant.flatMap(now => failures.update(_ + (id -> (e, now)))))
              .onExit(e => promise.done(e) *> inflight.update(_ - id))

  private def fail(app: AppEntry)(detail: String) = AgentError.Manifest(app.name.value, detail)

  private def key(value: String): S3Key = S3Key.from(value).toOption.get

  private def manifestKey(app: AppEntry, commit: CommitSha) = key(s"iae/manifests/${app.name.value}/${commit.value}.json")

  private def read(k: S3Key): IO[S3Error, Option[String]] =
    s3.headObject(bucket, k).flatMap:
      case None    => ZIO.none
      case Some(_) => s3.getObject(bucket, k).run(ZSink.collectAll[Byte]).map(b => Some(String(b.toArray, UTF_8)))

  private def cached(app: AppEntry, commit: CommitSha, image: Option[String]): IO[AgentError, Option[Manifest]] =
    read(manifestKey(app, commit)).mapError(e => fail(app)(s"reading the cached manifest: $e")).flatMap(ZIO.foreach(_)(ManifestSource.parse(app, _, image)))

  private def build(dir: Path, app: AppEntry, commit: CommitSha, image: Option[String]): IO[AgentError, Manifest] =
    val token  = s"${app.name.value}-${commit.short}-${java.util.UUID.randomUUID.toString.take(8)}"
    val name   = s"iae-build-$token"
    val output = s"iae-manifest-$token.json"
    val source = key(s"iae/sources/${commit.value}.zip")
    val project = CreateProject(
      name,
      Source("S3", Some(s"${bucket.value}/${source.value}"), Some(CodeBuildManifests.buildspec(app, output))),
      Artifacts("S3", Some(bucket.value), Some(s"iae/builds/$token"), Some("out"), Some("NONE"), Some("NONE")),
      Environment("LINUX_CONTAINER", app.image.getOrElse(this.image), "BUILD_GENERAL1_SMALL",
        Some(List(EnvironmentVariable("IAE_COMMIT", commit.value), EnvironmentVariable("IAE_APP", app.name.value)) ++
          image.filter(_ => app.pushesImage).map(EnvironmentVariable("IAE_IMAGE", _)))),
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
        _        <- pushed(app, id, image)
        manifest <- ManifestSource.parse(app, out, image)
        _        <- s3.putObject(bucket, manifestKey(app, commit), Chunk.fromArray(out.getBytes(UTF_8)), Some("application/json")).mapError(e => fail(app)(e.toString))
      yield manifest

  /** An app whose build pushes its image must have pushed it. */
  private def pushed(app: AppEntry, id: BuildId, image: Option[String]): IO[AgentError, Unit] =
    ZIO.foreachDiscard(image.filter(_ => app.pushesImage).zip(registry)): (image, ecr) =>
      val tag = image.substring(image.lastIndexOf(':') + 1)
      ecr.hasImage(CodeBuildManifests.repository(app.name), tag)
        .mapError(e => fail(app)(s"checking for the image: ${e.message}"))
        .filterOrFail(identity)(fail(app)(s"build ${id.value} succeeded but didn't push $image"))

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
  def make(codebuild: CodeBuildClient, s3: S3Client, bucket: BucketName, serviceRole: String, image: String, registry: Option[EcrClient] = None)
      : IO[AgentError, CodeBuildManifests] =
    for
      exists <- s3.bucketExists(bucket).mapError(e => AgentError.Config(s"build bucket: $e"))
      _      <- s3.createBucket(bucket).mapError(e => AgentError.Config(s"creating the build bucket: $e")).unless(exists)
      inflight <- Ref.Synchronized.make(Map.empty[String, Promise[AgentError, Manifest]])
      failures <- Ref.make(Map.empty[String, (AgentError, java.time.Instant)])
      repositories <- Ref.Synchronized.make(Map.empty[(AppName, Set[String]), String])
    yield CodeBuildManifests(codebuild, s3, bucket, serviceRole, image, registry, repositories, inflight, failures)

  /** The ECR repository of an app whose build pushes its image. */
  def repository(app: AppName): String = s"iae/${app.value}"

  /** The account of a role ARN. */
  def accountOf(roleArn: String): Option[String] = roleArn.split(':').lift(4).filter(_.nonEmpty)

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
