package zio.iae

import zio.test.*

object InfraSpec extends ZIOSpecDefault:
  object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
  object Thumbs  extends Bucket.Id(LogicalId("thumbs"))
  object Clash   extends Bucket.Id(LogicalId("uploads"))

  val app = AppName("photo-app")

  def spec = suite("Infra")(
    test("combining declarations merges resources and grants"):
      val infra = Bucket.readWrite(Uploads) ++ Bucket.reader(Thumbs) ++ Bucket.reader(Uploads)
      val manifest = infra.manifest(app)
      assertTrue(
        manifest.map(_.resources.map(_.id)) == Right(List(LogicalId("thumbs"), LogicalId("uploads"))),
        manifest.map(_.grants) == Right(
          List(
            Grant(LogicalId("thumbs"), ResourceKind.Bucket, Capability.BucketRead),
            Grant(LogicalId("uploads"), ResourceKind.Bucket, Capability.BucketRead),
            Grant(LogicalId("uploads"), ResourceKind.Bucket, Capability.BucketWrite),
          )
        ),
      )
    ,
    test("one logical id with two different specs is a conflict"):
      val manifest = (Bucket.readWrite(Uploads) ++ Bucket.readWrite(Clash)).manifest(app)
      assertTrue(manifest match
        case Left(InfraError.ConflictingDeclarations(id, specs)) => id == LogicalId("uploads") && specs.size == 2
        case _                                                   => false
      )
    ,
    test("the manifest round-trips through JSON"):
      val manifest = (Bucket.readWrite(Uploads) ++ Bucket.reader(Thumbs)).manifest(app)
      assertTrue(manifest.flatMap(m => Manifest.fromJson(m.toJson).left.map(InfraError.InvalidConfig(_))) == manifest)
    ,
    test("the spec hash is stable and changes with the spec"):
      val enabled  = ResourceSpec.Bucket(BucketSpec(versioning = Versioning.Enabled))
      val disabled = ResourceSpec.Bucket(BucketSpec(versioning = Versioning.Disabled))
      assertTrue(
        SpecHash.of(enabled) == SpecHash.of(enabled),
        SpecHash.of(enabled) != SpecHash.of(disabled),
        SpecHash.from(SpecHash.of(enabled).value).contains(SpecHash.of(enabled)),
      )
    ,
  )
