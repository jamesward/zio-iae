package zio.iae.aws

import zio.iae.*
import zio.iae.aws.client.*
import zio.test.*

object BucketDiffSpec extends ZIOSpecDefault:
  val address  = Address(AppName("photo-app"), EnvName("test"), ResourceKind.Bucket, LogicalId("uploads"))
  val name     = BucketName.from("photo-app-test-uploads").toOption.get
  val provider = BucketProvider

  def observed(spec: BucketSpec, versioning: VersioningStatus, extraTags: Map[String, String] = Map.empty) =
    ObservedBucket(name, versioning, ManagedTags.of(address, SpecHash.of(ResourceSpec.Bucket(spec))) ++ extraTags)

  def spec = suite("BucketProvider.diff")(
    test("a missing bucket is created"):
      assertTrue(provider.diff(address, BucketSpec(), None) == Some(Change.Create(address, BucketSpec())))
    ,
    test("a bucket that matches its spec needs no change, whatever other tags it has"):
      val desired = BucketSpec(versioning = Versioning.Enabled)
      assertTrue(provider.diff(address, desired, Some(observed(desired, VersioningStatus.Enabled, Map("team" -> "x")))).isEmpty)
    ,
    test("never-enabled and suspended versioning both satisfy 'disabled'"):
      assertTrue(
        provider.diff(address, BucketSpec(), Some(observed(BucketSpec(), VersioningStatus.Off))).isEmpty,
        provider.diff(address, BucketSpec(), Some(observed(BucketSpec(), VersioningStatus.Suspended))).isEmpty,
      )
    ,
    test("turning on versioning is an update that names the field and the new spec hash"):
      val desired = BucketSpec(versioning = Versioning.Enabled)
      val change  = provider.diff(address, desired, Some(observed(BucketSpec(), VersioningStatus.Off)))
      assertTrue(change match
        case Some(Change.Update(_, `desired`, fields)) =>
          fields.exists(_.field == "versioning") && fields.exists(_.field == s"tag ${TagKey.Spec.wire}")
        case _ => false
      )
    ,
    test("an unmanaged bucket with the right name gets tagged"):
      val bare   = ObservedBucket(name, VersioningStatus.Off, Map.empty)
      val change = provider.diff(address, BucketSpec(), Some(bare))
      assertTrue(change match
        case Some(Change.Update(_, _, fields)) => fields.size == TagKey.values.size
        case _                                 => false
      )
    ,
    test("risk follows the change type"):
      assertTrue(
        provider.risk(Change.Create(address, BucketSpec())) == Risk.Low,
        provider.risk(Change.Delete(address)) == Risk.Critical,
      )
    ,
  )
