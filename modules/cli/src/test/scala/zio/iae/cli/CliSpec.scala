package zio.iae.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.{AwsContext, BucketNaming}
import zio.iae.aws.client.*
import zio.iae.engine.*
import zio.iae.testkit.FlociLayer
import zio.test.*

object CliSpec extends ZIOSpecDefault:
  object Uploads          extends Bucket.Id(LogicalId("uploads"))
  object UploadsVersioned extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))

  val app = AppName("cli-app")

  def manifestFile(infra: Infra[?]): Task[Path] =
    for
      manifest <- ZIO.fromEither(infra.manifest(app)).mapError(e => IllegalStateException(e.message))
      path     <- ZIO.attemptBlocking(Files.createTempFile("manifest", ".json").nn)
      _        <- ZIO.attemptBlocking(Files.writeString(path, manifest.toJson, UTF_8))
    yield path

  def args(command: String, env: String, manifest: Path, extra: String*): List[String] =
    List(command, "--env", env, "--manifest", manifest.toString) ++ extra

  def run(arguments: List[String]): ZIO[Engine, Throwable, (Int, String)] =
    for
      command <- ZIO.fromEither(Command.parse(arguments)).mapError(IllegalArgumentException(_))
      _       <- TestConsole.clearOutput
      code    <- Run.execute(command).mapError(e => IllegalStateException(e.message))
      output  <- TestConsole.output
    yield (code.code, output.mkString)

  val engine: ZLayer[Any, Throwable, Engine] =
    ZLayer.make[Engine](
      FlociLayer.live,
      Client.default,
      S3Client.layer,
      AwsJsonClient.layer,
      TaggingClient.layer,
      SqsClient.layer,
      AwsQueryClient.layer,
      IamClient.layer,
      StsClient.layer,
      AwsContext.layer.mapError(e => IllegalStateException(e.message)),
      ZLayer.succeed[CommitOrder](GitCommitOrder(Paths.get(".").nn)),
      AwsEngine.layer(BucketNaming(None)),
    )

  def spec = suite("iae")(
    suite("arguments")(
      test("plan and apply need an env and accept a manifest file, a commit and a phase"):
        assertTrue(
          Command.parse(List("plan", "--env", "dev")) match
            case Right(Command.Plan(target, phases, false)) => target.manifest == ManifestSource.Stdin && phases.size == 2
            case _                                          => false
          ,
          Command.parse(List("plan", "--env", "prod", "--manifest", "m.json", "--phase", "contract", "--json")) match
            case Right(Command.Plan(target, List(Phase.Contract), true)) => target.manifest == ManifestSource.File(Paths.get("m.json").nn)
            case _                                                      => false
          ,
          Command.parse(List("apply", "--env", "dev", "--commit", "a" * 40)).isRight,
        )
      ,
      test("bad arguments are explained"):
        assertTrue(
          Command.parse(List("plan")) == Left("--env is required"),
          Command.parse(List("plan", "--env", "Prod")).isLeft,
          Command.parse(List("plan", "--env", "dev", "--commit", "abc")).isLeft,
          Command.parse(List("apply", "--env", "dev", "--break-glass", " ")) == Left("--break-glass needs a reason"),
          Command.parse(List("deploy")) == Left("unknown command 'deploy'"),
        )
      ,
    ),
    suite("on Floci")(
      test("dev applies without asking, then there is nothing left to do"):
        for
          manifest   <- manifestFile(Bucket.readWrite(Uploads))
          (planned, planOut) <- run(args("plan", "dev", manifest))
          (applied, applyOut) <- run(args("apply", "dev", manifest))
          (_, after) <- run(args("plan", "dev", manifest))
        yield assertTrue(
          planned == 0,
          planOut.contains("+ create  Bucket uploads [Low]"),
          applied == 0,
          planOut.contains("+ create  Role runtime [Low]"),
          applyOut.contains("applied 2 change(s)"),
          after.contains("Expand plan") && after.contains("nothing to change"),
        )
      ,
      test("a medium-risk change in prod needs approval, unless the glass is broken"):
        for
          v1                   <- manifestFile(Bucket.readWrite(Uploads))
          v2                   <- manifestFile(Bucket.readWrite(UploadsVersioned))
          (created, _)         <- run(args("apply", "prod", v1))
          (refused, refusedOut) <- run(args("apply", "prod", v2))
          (broken, brokenOut)  <- run(args("apply", "prod", v2, "--break-glass", "incident 42"))
          (_, after)           <- run(args("plan", "prod", v2, "--phase", "expand"))
        yield assertTrue(
          created == 0,
          refused == Run.needsApproval.code,
          refusedOut.contains("needs 1 approval(s) from @approvers") && refusedOut.contains("not applied"),
          broken == 0,
          brokenOut.contains("break-glass: applying anyway (incident 42)"),
          after.contains("nothing to change"),
        )
      ,
    ).provideShared(engine) @@ TestAspect.sequential @@ TestAspect.withLiveClock,
  )
