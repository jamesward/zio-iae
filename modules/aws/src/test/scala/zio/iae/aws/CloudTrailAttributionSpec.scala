package zio.iae.aws

import java.time.Instant
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.CloudTrailFake
import zio.test.*

/** Drift attribution: a resource's write calls by people, not reads and not the pipeline's deployer. */
object CloudTrailAttributionSpec extends ZIOSpecDefault:
  val address = Address(AppName("photo-app"), EnvName("prod"), ResourceKind.Bucket, LogicalId("uploads"))

  def spec = suite("CloudTrail attribution")(
    test("a hand change is attributed to its caller; reads and the deployer's own calls aren't"):
      ZIO.scoped:
        for
          fake   <- CloudTrailFake.make.orDie
          http   <- ZIO.service[Client]
          config  = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), Endpoint.parse(fake.url.encode).toOption)
          bucket  = BucketNaming(None).physical(address).toOption.get.value
          at      = Instant.parse("2026-11-02T10:30:00Z").nn
          _      <- fake.record(bucket, "PutBucketVersioning", "arn:aws:sts::111111111111:assumed-role/Admin/alice", at)
          _      <- fake.record(bucket, "GetBucketVersioning", "arn:aws:sts::111111111111:assumed-role/Admin/alice", at, readOnly = true)
          _      <- fake.record(bucket, "PutBucketVersioning", "arn:aws:sts::111111111111:assumed-role/iae-deployer/iae", at.minusSeconds(3600).nn)
          _      <- fake.record("someone-elses-bucket", "PutBucketPolicy", "arn:aws:iam::111111111111:user/bob", at)
          found  <- CloudTrailAttribution(CloudTrailClient(AwsJsonClient(http, config)), BucketNaming(None), Some("iae-deployer"))
                      .changes(address, at.minusSeconds(86400).nn)
        yield assertTrue(found == List(ChangedBy("PutBucketVersioning", "arn:aws:sts::111111111111:assumed-role/Admin/alice", at))),
  ).provideShared(Client.default)
