package zio.iae.aws

import zio.NonEmptyChunk
import zio.iae.*
import zio.iae.aws.client.*
import zio.test.*

object BucketDiffSpec extends ZIOSpecDefault:
  val address  = Address(AppName("photo-app"), EnvName("test"), ResourceKind.Bucket, LogicalId("uploads"))
  val name     = BucketName.from("photo-app-test-uploads").toOption.get
  val provider = BucketProvider

  def observed(spec: BucketSpec, versioning: VersioningStatus, extraTags: Map[String, String] = Map.empty) =
    ObservedBucket(name, versioning, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Bucket(spec))) ++ extraTags)

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
        case Some(Change.Update(_, _, fields)) => fields.size == ManagedTags.identity(address, SpecHash.of(ResourceSpec.Bucket(BucketSpec()))).size
        case _                                 => false
      )
    ,
    test("a re-declared orphan is an update that clears the orphan mark"):
      val tags   = observed(BucketSpec(), VersioningStatus.Off).tags + (TagKey.Orphaned.wire -> "2026-10-01")
      val change = provider.diff(address, BucketSpec(), Some(ObservedBucket(name, VersioningStatus.Off, tags)))
      assertTrue(change match
        case Some(Change.Update(_, _, fields)) => fields.toList == List(FieldChange("orphaned", "2026-10-01", "(none)"))
        case _                                 => false
      )
    ,
    test("converging keeps the user's tags, records the deploying commit and keeps the old one otherwise"):
      val hash    = SpecHash.of(ResourceSpec.Bucket(BucketSpec()))
      val older   = CommitSha.from("a" * 40).toOption.get
      val newer   = CommitSha.from("b" * 40).toOption.get
      val current = Map("team" -> "photos", TagKey.Commit.wire -> older.value, TagKey.Orphaned.wire -> "2026-10-01")
      val deploy  = ManagedTags.converged(current, address, hash, ApplyContext(Some(newer)))
      val local   = ManagedTags.converged(current, address, hash, ApplyContext.local)
      assertTrue(
        deploy.get("team").contains("photos"),
        ManagedTags.commit(deploy).contains(newer),
        ManagedTags.commit(local).contains(older),
        !deploy.contains(TagKey.Orphaned.wire),
        ManagedTags.specHash(deploy).contains(hash),
      )
    ,
    test("risk follows the change type"):
      assertTrue(
        provider.assess(Change.Create(address, BucketSpec())) == Assessment(Risk.Low, Set.empty),
        provider.assess(Change.Delete(address)) == Assessment(Risk.Critical, Set(ChangeFlag.DataLoss)),
        // Tiering keeps every object; expiring them touches retention.
        provider.assess(Change.Update(address, BucketSpec(), NonEmptyChunk(FieldChange("lifecycle", "none", "Intelligent-Tiering after 30 days")))) ==
          Assessment(Risk.Medium, Set.empty),
        provider.assess(Change.Update(address, BucketSpec(), NonEmptyChunk(FieldChange("lifecycle", "none", "expire after 30 days")))) ==
          Assessment(Risk.Critical, Set(ChangeFlag.DataLoss)),
      )
    ,
  )
