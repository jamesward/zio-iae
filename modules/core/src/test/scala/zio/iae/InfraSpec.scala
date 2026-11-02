package zio.iae

import zio.schema.derived
import zio.test.*

object InfraSpec extends ZIOSpecDefault:
  object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
  object Thumbs  extends Bucket.Id(LogicalId("thumbs"))
  object Clash   extends Bucket.Id(LogicalId("uploads"))

  val app = AppName("photo-app")

  final case class Resize(photo: String) derives zio.schema.Schema
  final case class Other(n: Int) derives zio.schema.Schema
  object Thumbnailer extends MessageQueue.Id[Resize](LogicalId("thumbnailer"))
  object Watched extends Service.Id(LogicalId("web"), ServiceSpec(8080, canary = CanarySpec(alarms = List(AppAlarm("uploads", "UploadFailures", 1)))))
  object Unwatched extends Service.Id(LogicalId("web"), ServiceSpec(8080))
  object BadAlarm extends Service.Id(LogicalId("web"), ServiceSpec(8080, canary = CanarySpec(alarms = List(AppAlarm("Upload Failures", "UploadFailures", 1)))))
  object Indexer     extends MessageQueue.Id[Resize](LogicalId("indexer"))
  object PhotoEvents extends Topic.Id[Resize](LogicalId("photo-events"), List(Topic.to(Thumbnailer), Topic.to(Indexer)))

  def spec = suite("Infra")(
    test("a service with alarms of its own may publish their metrics; one without may not"):
      val watched   = Service.run(Watched).manifest(app).map(_.grants)
      val unwatched = Service.run(Unwatched).manifest(app).map(_.grants)
      assertTrue(watched == Right(List(Grant(LogicalId("web"), ResourceKind.Service, Capability.MetricsPublish))), unwatched == Right(Nil))
    ,
    test("an alarm name that can't be part of an alarm's name is refused"):
      assertTrue(Service.run(BadAlarm).manifest(app).left.exists(_.toString.contains("Upload Failures")))
    ,
    test("combining declarations merges resources and grants"):
      val infra = Bucket.readWrite(Uploads) ++ Bucket.reader(Thumbs) ++ Bucket.reader(Uploads)
      val manifest = infra.manifest(app)
      assertTrue(
        manifest.map(_.resources.map(_.id)) == Right(List(LogicalId("thumbs"), LogicalId("uploads"), Manifest.runtimeRole)),
        manifest.map(_.resources.last.spec) == manifest.map(m => ResourceSpec.Role(RoleSpec(m.grants, List(TrustedService.EcsTasks)))),
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
    test("a topic's subscribers must be queues the app declares"):
      val missing = Topic.publisher(PhotoEvents).manifest(app)
      val wired   = (Topic.publisher(PhotoEvents) ++ MessageQueue.wired(Thumbnailer) ++ MessageQueue.receiver(Indexer)).manifest(app)
      assertTrue(
        missing match
          case Left(InfraError.InvalidConfig(detail)) => detail.contains("indexer") && detail.contains("thumbnailer")
          case _                                      => false
        ,
        wired.map(_.resources.map(_.kind)).map(_.toSet) == Right(Set(ResourceKind.Topic, ResourceKind.Queue, ResourceKind.Role)),
        wired.map(_.grants.map(_.capability)) == Right(List(Capability.QueueReceive, Capability.TopicPublish)),
      )
    ,
    test("a topic can only deliver to queues of its message type"):
      typeCheck("""
        object OtherQueue extends MessageQueue.Id[Other](LogicalId("other"))
        object Bad extends Topic.Id[Resize](LogicalId("bad"), List(Topic.to(OtherQueue)))
      """).map(result => assertTrue(result.isLeft))
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
