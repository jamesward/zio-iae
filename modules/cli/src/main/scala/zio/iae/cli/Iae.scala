package zio.iae.cli

import zio.*
import zio.http.Client
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.*
import zio.iae.engine.*

/** The `iae` command line: the engine for people, outside the agent. */
object Iae extends ZIOAppDefault:
  def run: ZIO[ZIOAppArgs, Any, Any] =
    getArgs.flatMap: args =>
      Command.parse(args.toList) match
        case Left(error) => Console.printLineError(s"$error\n\n${Command.usage}").as(ExitCode.failure).flatMap(exit)
        case Right(command) =>
          Run
            .execute(command)
            .provide(engine(command))
            .catchAll(e => Console.printLineError(s"iae: ${e.message}").as(ExitCode.failure))
            .flatMap(exit)

  private def engine(command: Command): ZLayer[Any, CliError, Engine] =
    val repository = command match
      case Command.Plan(target, _, _) => target.repository
      case Command.Apply(target, _)   => target.repository
      case Command.Help               => java.nio.file.Paths.get(".").nn
    val failed = (detail: String) => CliError.Failed(EngineError.CommitOrder(detail))
    val naming = ZLayer.fromZIO(System.env("IAE_NAME_SUFFIX").orElseFail(failed("cannot read IAE_NAME_SUFFIX")).map(BucketNaming(_)))
    val clients = AwsConfig.layer.mapError(e => failed(e.message)) ++ Client.default.mapError(e => failed(s"HTTP client: ${e.getMessage}"))
    val aws     = clients >>> AwsEngine.clients.mapError(e => failed(s"connecting to AWS: ${e.message}"))
    naming.flatMap(n => (aws ++ ZLayer.succeed[CommitOrder](GitCommitOrder(repository))) >>> AwsEngine.layer(n.get))
