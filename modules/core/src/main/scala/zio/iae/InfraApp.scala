package zio.iae

import zio.*

/** A program whose environment is exactly the infra it declares. Running it with `--iae-manifest` prints the manifest
  * as JSON without touching the cloud; otherwise it binds the infra and runs `program`.
  */
abstract class InfraApp[R](val appName: AppName, val infra: Infra[R]) extends ZIOAppDefault:
  def cloud: ZLayer[Any, InfraError, Cloud]

  def program: ZIO[R, Any, Any]

  final def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    getArgs.flatMap: args =>
      if args.contains(InfraApp.manifestFlag) then printManifest
      else
        program
          .provideLayer(cloud >>> infra.layer)
          .catchSome:
            case error: InfraError => ZIO.logError(error.message) *> exit(ExitCode.failure)

  private def printManifest: IO[Any, Unit] =
    ZIO.fromEither(infra.manifest(appName)).flatMap(m => Console.printLine(m.toJson))

object InfraApp:
  val manifestFlag = "--iae-manifest"
