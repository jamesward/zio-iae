package zio.iae.testkit

import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.model.PushResponseItem
import org.testcontainers.DockerClientFactory
import zio.*
import zio.iae.aws.client.*

/** Pushes images on the local Docker daemon into a Floci's ECR, as a build's `docker push` would. Floci serves its
  * registry path-style on `localhost:<port>/<account>/<region>/<repository>`, which Docker pushes to over plain HTTP.
  */
object FlociRegistry:
  def push(config: AwsConfig, account: String, local: String, repository: String, tag: String): Task[Unit] =
    for
      endpoint <- ZIO.fromOption(config.endpoint).orElseFail(IllegalArgumentException("the config has no Floci endpoint"))
      target    = s"localhost:${endpoint.port.getOrElse(4566)}/$account/${config.region.value}/$repository"
      _ <- ZIO.attemptBlocking:
             val docker = DockerClientFactory.instance().nn.client().nn
             docker.tagImageCmd(local, target, tag).nn.exec()
             docker.pushImageCmd(target).nn.withTag(tag).nn.exec(ResultCallback.Adapter[PushResponseItem]()).nn.awaitCompletion()
    yield ()
