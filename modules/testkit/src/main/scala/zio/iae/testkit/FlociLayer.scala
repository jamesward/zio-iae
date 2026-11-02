package zio.iae.testkit

import io.floci.testcontainers.FlociContainer
import org.testcontainers.containers.Network
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
          // The Docker socket lets Floci run CodeBuild builds and ECS tasks as real containers. A network of its own
          // lets it reach its ECR registry (another container) by name, which Docker's default bridge doesn't. Path-style
          // repository URIs (localhost:<port>/<account>/<region>/<repo>) are the ones every Docker daemon pulls over plain
          // HTTP; one with the containerd image store takes `*.localhost` for HTTPS.
          val floci = FlociContainer(DockerImageName.parse(image)).withDockerSocket(true).nn.withNetwork(Network.newNetwork()).nn
            .withEnv("FLOCI_SERVICES_ECR_URI_STYLE", "path").nn
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
