package zio.iae.testkit

import io.floci.testcontainers.FlociContainer
import org.testcontainers.utility.DockerImageName
import zio.*
import zio.iae.aws.client.*

/** Floci, the local AWS emulator, as ZIO layers. Share one container per suite with `provideShared`. */
object FlociLayer:
  val image: String = "floci/floci:2.4.0"

  val container: ZLayer[Any, Throwable, FlociContainer] =
    ZLayer.scoped:
      ZIO.acquireRelease(
        ZIO.attemptBlocking:
          // The Docker socket lets Floci run CodeBuild builds (and later ECS tasks) as real containers.
          val floci = FlociContainer(DockerImageName.parse(image)).withDockerSocket(true).nn
          floci.start()
          floci
      )(floci => ZIO.attemptBlocking(floci.stop()).ignoreLogged)

  val awsConfig: ZLayer[FlociContainer, Throwable, AwsConfig] =
    ZLayer:
      ZIO.serviceWithZIO[FlociContainer]: floci =>
        val config =
          for
            region   <- Region.from(floci.getRegion.nn)
            endpoint <- Endpoint.parse(floci.getEndpoint.nn)
          yield AwsConfig.static(
            region,
            AwsCredentials(AccessKeyId(floci.getAccessKey.nn), SecretAccessKey(floci.getSecretKey.nn), None),
            Some(endpoint),
          )
        ZIO.fromEither(config).mapError(IllegalStateException(_))

  /** A running Floci and an `AwsConfig` pointing at it. */
  val live: ZLayer[Any, Throwable, AwsConfig] = container >>> awsConfig
