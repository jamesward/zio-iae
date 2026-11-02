package zio.iae.aws

import zio.NonEmptyChunk
import zio.iae.*
import zio.test.*

/** Drift: what differs from a spec the resource already records was changed outside the pipeline. */
object DriftedSpec extends ZIOSpecDefault:
  val bucket  = Address(AppName("photo-app"), EnvName("prod"), ResourceKind.Bucket, LogicalId("uploads"))
  val service = bucket.copy(kind = ResourceKind.Service, id = LogicalId("web"))

  def spec = suite("Drifted")(
    test("a field changed under an unchanged spec hash is drift; a new spec isn't"):
      val handChange = Change.Update(bucket, BucketSpec(), NonEmptyChunk(FieldChange("versioning", "Enabled", "Disabled")))
      val newSpec = Change.Update(
        bucket,
        BucketSpec(),
        NonEmptyChunk(FieldChange("versioning", "Enabled", "Disabled"), FieldChange(s"tag ${TagKey.Spec.wire}", "aaaa", "bbbb")),
      )
      assertTrue(Drifted.flags(handChange) == Set(ChangeFlag.Drift), Drifted.flags(newSpec).isEmpty, Drifted.flags(Change.Create(bucket, BucketSpec())).isEmpty)
    ,
    test("a service's version waiting for a fix isn't drift"):
      val version = Change.Update(service, ServiceSpec(80), NonEmptyChunk(FieldChange("version", "v1", "v2")))
      assertTrue(Drifted.flags(version).isEmpty),
  )
