package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import zio.*
import zio.http.*
import zio.iae.*
import zio.iae.agent.harness.*
import zio.iae.aws.client.{AwsConfig, AwsError}
import zio.iae.engine.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** The whole loop through the harness, over HTTP: a push from the UI goes through the signed webhook and the SQS
  * inbox to the agent, which asks for approval in (fake) Slack; an approver's ✅ from the UI gets it applied.
  */
object HarnessSpec extends ZIOSpecDefault:
  object UploadsVersioned extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))

  val uniqueEnv: UIO[EnvName] =
    ZIO.collectAll(List.fill(6)(Random.nextIntBounded(26).map(i => ('a' + i).toChar))).map(cs => EnvName.from("h" + cs.mkString).toOption.get)

  def until[A](what: String)(check: IO[Any, Option[A]]): IO[String, A] =
    check.orElseSucceed(None).repeat(Schedule.spaced(500.millis) && Schedule.recurUntil[Option[A]](_.isDefined)).map(_._2).timeout(90.seconds).flatMap:
      case Some(Some(a)) => ZIO.succeed(a)
      case _             => ZIO.fail(s"timed out waiting for $what")

  def converged(runtime: EnvRuntime, manifest: Manifest): UIO[Option[Unit]] =
    ZIO.forall(List(Phase.Expand, Phase.Contract))(p => runtime.engine.plan(manifest, runtime.env, None, p).map(_.isEmpty)).map(Option.when(_)(())).orElseSucceed(None)

  def spec = suite("Harness")(
    test("push from the UI, approve in Slack from the UI, and the agent applies"):
      ZIO.scoped:
        for
          staging <- uniqueEnv
          prod    <- uniqueEnv
          harness <- Harness.make(0, List(staging, prod))
          v1       = Bucket.readWrite(Harness.Uploads).manifest(Harness.app).toOption.get
          _       <- until("v1 in prod")(converged(harness.envs(1), v1))
          v2       = Bucket.readWrite(UploadsVersioned).manifest(Harness.app).toOption.get
          _       <- harness.ui.push(Map(Harness.manifest -> v2.toJson), "turn on versioning")
          _       <- until("v2 in staging")(converged(harness.envs(0), v2))
          request <- until("an approval request")(harness.slack.messages.get.map(_.find(_.request.isDefined)))
          http    <- ZIO.service[Client]
          react    = (user: String) =>
                       http.batched(
                         Request.post(URL.decode(s"http://localhost:${harness.port}/ui/react").toOption.get, Body.fromString(s"""{"ts":"${request.ts}","user":"$user","reaction":"white_check_mark"}"""))
                       )
          carol   <- react("U_CAROL")
          _       <- ZIO.sleep(2.seconds)
          notYet  <- converged(harness.envs(1), v2)
          alice   <- react("U_ALICE")
          _       <- until("v2 in prod")(converged(harness.envs(1), v2))
          posted  <- harness.slack.messages.get
          page    <- http.batched(Request.get(URL.decode(s"http://localhost:${harness.port}/").toOption.get)).flatMap(_.body.asString)
        yield assertTrue(
          carol.status.isSuccess,
          notYet.isEmpty,
          alice.status.isSuccess,
          posted.exists(m => m.text.startsWith("Applied") && m.text.contains("<@U_ALICE>")),
          page.contains("iae harness"),
        )
    ,
    test("an unanswered approval expires when the clock moves on a day, and is asked again"):
      ZIO.scoped:
        for
          staging <- uniqueEnv
          prod    <- uniqueEnv
          harness <- Harness.make(0, List(staging, prod))
          v1       = Bucket.readWrite(Harness.Uploads).manifest(Harness.app).toOption.get
          _       <- until("v1 in prod")(converged(harness.envs(1), v1))
          v2       = Bucket.readWrite(UploadsVersioned).manifest(Harness.app).toOption.get
          _       <- harness.ui.push(Map(Harness.manifest -> v2.toJson), "turn on versioning")
          first   <- until("an approval request")(harness.slack.messages.get.map(_.find(_.request.isDefined)))
          _       <- harness.ui.advance(25.hours)
          second  <- until("a second approval request")(harness.slack.messages.get.map(_.filter(_.request.isDefined).find(_.ts != first.ts)))
        yield assertTrue(second.request.exists(_.expires > first.request.get.expires))
    ,
    test("staging and prod in accounts of their own, reached through assumed roles"):
      ZIO.scoped:
        for
          staging   <- uniqueEnv
          prod      <- uniqueEnv
          accounts  <- ZIO.foreach(List(staging, prod))(env => FlociLayer.live.build.map(env -> _.get[AwsConfig])).mapError(_.getMessage)
          harness   <- Harness.make(0, List(staging, prod), accounts = accounts.toMap)
          seed       = Bucket.readWrite(Harness.Uploads).manifest(Harness.app).toOption.get
          _         <- until("the seed in prod")(converged(harness.envs(1), seed))
          bucket     = (env: EnvName) => zio.iae.aws.BucketNaming(None).physical(Address(Harness.app, env, ResourceKind.Bucket, Harness.Uploads.logicalId)).toOption.get
          inStaging <- harness.s3(0).bucketExists(bucket(staging))
          inProd    <- harness.s3(1).bucketExists(bucket(prod))
          crossed   <- harness.s3(0).bucketExists(bucket(prod))
          tooling   <- ZIO.serviceWithZIO[zio.iae.aws.client.S3Client](_.bucketExists(bucket(staging)))
          evidence  <- harness.audit.all
        yield assertTrue(
          inStaging, inProd, !crossed, !tooling,
          evidence.keySet.map(_._1) == Set(s"assessment-${staging.value}", s"assessment-${prod.value}"),
          evidence.keySet.map(_._3) == Set("iae-cm-1", "iae-cm-3"),
        )
  ).provideShared(
    ((FlociLayer.live ++ Client.default) >+> AwsEngine.clients).mapError:
      case a: AwsError  => a.message
      case t: Throwable => t.getMessage
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
