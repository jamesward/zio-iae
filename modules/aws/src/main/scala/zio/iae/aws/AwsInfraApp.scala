package zio.iae.aws

import zio.*
import zio.iae.*

/** An `InfraApp` that binds its infra on AWS, configured from the environment (see `AwsCloud.fromEnvironment`). */
abstract class AwsInfraApp[R](appName: AppName, infra: Infra[R]) extends InfraApp[R](appName, infra):
  def cloud: ZLayer[Any, InfraError, Cloud] = AwsCloud.fromEnvironment(appName)
