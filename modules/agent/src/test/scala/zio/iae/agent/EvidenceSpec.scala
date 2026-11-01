package zio.iae.agent

import java.time.Instant
import zio.*
import zio.http.Client
import zio.iae.aws.client.*
import zio.iae.testkit.{AuditManagerFake, FlociLayer}
import zio.stream.ZSink
import zio.test.*

object EvidenceSpec extends ZIOSpecDefault:
  val change = ChangeEvidence(
    "photo-app", "prod", "a" * 40, "dev@example.com", Some("U_DEV"), Some("b" * 40), "c" * 64, "Expand",
    List("~ update  Bucket uploads [Medium]"), "needs 1 approval(s) from @infra-approvers", List("0:...: approver needed"),
    Some(ApprovalEvidence("C_PHOTOS", "1.000001", List("U_ALICE"))), List("photo-app/prod/Bucket/uploads"), Instant.EPOCH.nn,
  )

  def fakeConfig(fake: AuditManagerFake, base: AwsConfig): AwsConfig = base.copy(endpoint = Endpoint.parse(fake.url.encode).toOption)

  def spec = suite("Evidence")(
    test("each control's document goes to the evidence bucket and is imported into its control"):
      ZIO.scoped:
        for
          fake    <- AuditManagerFake.make
          aws     <- ZIO.service[AwsConfig]
          http    <- ZIO.service[Client]
          suffix  <- Random.nextIntBounded(1000000)
          bucket   = BucketName.from(s"iae-evidence-$suffix").toOption.get
          s3       = S3Client(http, aws)
          _       <- s3.createBucket(bucket)
          settings = AuditSettings(bucket.value, "assessment-1", "IAE Change Management", Map("IAE-CM-1" -> "ctl-1", "IAE-CM-3" -> "ctl-3"))
          _       <- EvidenceRecorder(s3, AuditManagerClient(http, fakeConfig(fake, aws)), settings).record(change)
          cm1     <- fake.evidence("assessment-1", "IAE Change Management", "ctl-1")
          cm3     <- fake.evidence("assessment-1", "IAE Change Management", "ctl-3")
          key      = S3Key.from(cm3.head.stripPrefix(s"s3://${bucket.value}/")).toOption.get
          doc     <- s3.getObject(bucket, key).run(ZSink.collectAll[Byte]).map(b => String(b.toArray, "UTF-8"))
        yield assertTrue(
          cm1.size == 1,
          cm3.size == 1,
          cm3.head.startsWith(s"s3://${bucket.value}/iae/evidence/prod/photo-app/"),
          doc.contains("\"control\":\"IAE-CM-3\""),
          doc.contains("U_ALICE"),
        ),
    test("more than 100 imports to a control in a day are refused"):
      ZIO.scoped:
        for
          fake   <- AuditManagerFake.make
          aws    <- ZIO.service[AwsConfig]
          http   <- ZIO.service[Client]
          client  = AuditManagerClient(http, fakeConfig(fake, aws))
          first  <- client.importEvidence("a", "set", "c", List.tabulate(99)(i => s"s3://b/$i"))
          second <- client.importEvidence("a", "set", "c", List("s3://b/99", "s3://b/100"))
        yield assertTrue(first.isEmpty, second.size == 1, second.head.contains("s3://b/100")),
  ).provideShared(FlociLayer.live.mapError(_.getMessage), Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveRandom @@ TestAspect.withLiveClock
