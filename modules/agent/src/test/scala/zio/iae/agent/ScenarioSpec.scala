package zio.iae.agent

import zio.*
import zio.http.*
import zio.iae.*
import zio.iae.agent.harness.*
import zio.iae.aws.client.{AwsConfig, AwsError}
import zio.iae.engine.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** Every built-in harness scenario, run headlessly against its own harness: what the UI's buttons do, as tests. */
object ScenarioSpec extends ZIOSpecDefault:
  def run(scenario: Scenario): ZIO[Scope & AwsConfig & AwsEngine.Clients & Client, String, TestResult] =
    for
      staging <- HarnessSpec.uniqueEnv
      prod    <- HarnessSpec.uniqueEnv
      harness <- Harness.make(0, List(staging, prod))
      seed     = Bucket.readWrite(Harness.Uploads).manifest(Harness.app).toOption.get
      _       <- HarnessSpec.until("the seed in prod")(HarnessSpec.converged(harness.envs(1), seed))
      _       <- Scenario.run(harness, scenario, _ => ZIO.unit)
    yield assertCompletes

  def spec = suite("Scenarios")(
    Scenario.all.map(s => test(s.name)(ZIO.scoped(run(s))))*
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >+> AwsEngine.clients).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(8.minutes)
