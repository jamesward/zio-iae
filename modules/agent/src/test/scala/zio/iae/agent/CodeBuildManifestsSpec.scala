package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** Manifests built in Floci's CodeBuild, which runs each build as a real container. */
object CodeBuildManifestsSpec extends ZIOSpecDefault:
  object Uploads extends Bucket.Id(LogicalId("uploads"))

  val app      = AppName("photo-app")
  val manifest = Bucket.readWrite(Uploads).manifest(app).toOption.get

  def config(entry: AppEntry): RepoConfig =
    RepoConfig(List(entry), List(EnvName("dev")), Nil, SlackSettings("C", zio.iae.engine.policy.SlackGroup.from("owners").toOption.get, Nil))

  val builds: ZIO[AwsConfig & Client, String, CodeBuildManifests] =
    for
      aws    <- ZIO.service[AwsConfig]
      http   <- ZIO.service[Client]
      suffix <- Random.nextIntBounded(1000000)
      bucket  = BucketName.from(s"iae-builds-$suffix").toOption.get
      builds <- CodeBuildManifests.make(CodeBuildClient(AwsJsonClient(http, aws)), S3Client(http, aws), bucket, "arn:aws:iam::000000000000:role/iae-build", "alpine:3.20")
                  .mapError(_.message)
    yield builds

  def artifacts(builds: CodeBuildManifests): IO[String, Int] =
    builds.s3.listObjects(builds.bucket, None).runCollect.mapBoth(_.toString, _.count(_.key.value.contains("iae-manifest-")))

  def spec = suite("CodeBuild manifests")(
    test("a commit is built once in CodeBuild, through its build commands, and its manifest is kept"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          entry    = AppEntry(app, List("cat", "out/manifest.json"), build = List("mkdir -p out", "cp .iae/manifest.json out/manifest.json"), image = Some("alpine:3.20"))
          commit  <- repo.commit(Map(".iae/manifest.json" -> manifest.toJson), "seed")
          ws       = repo.workspace(EnvName("dev"))
          _       <- ws.sync.mapError(_.message)
          builds  <- builds
          first   <- builds.manifests(ws.dir, config(entry), commit).mapError(_.message)
          again   <- builds.manifests(ws.dir, config(entry), commit).mapError(_.message)
          count   <- artifacts(builds)
        yield assertTrue(first == List(manifest), again == first, count == 1),
    test("a failing build is reported with its phase"):
      ZIO.scoped:
        for
          repo   <- TestRepo.make
          entry   = AppEntry(app, List("cat", ".iae/manifest.json"), build = List("echo compiling; exit 3"), image = Some("alpine:3.20"))
          commit <- repo.commit(Map(".iae/manifest.json" -> manifest.toJson), "seed")
          ws      = repo.workspace(EnvName("dev"))
          _      <- ws.sync.mapError(_.message)
          builds <- builds
          error  <- builds.manifests(ws.dir, config(entry), commit).flip
          start  <- Clock.ClockLive.nanoTime
          again  <- builds.manifests(ws.dir, config(entry), commit).flip
          took   <- Clock.ClockLive.nanoTime.map(n => Duration.fromNanos(n - start))
          kept   <- builds.failures.get
        yield assertTrue(
          error.message.contains("failed"),
          error.message.contains("BUILD"),
          again == error,
          took < 2.seconds,
          kept.keySet == Set(s"photo-app@${commit.value}"),
        ),
  ).provideShared(FlociLayer.live.mapError(_.getMessage), Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
    @@ TestAspect.timeout(5.minutes)
