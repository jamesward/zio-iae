package photos

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.{AwsJsonClient, DynamoClient, S3Client, SecretsClient, SqsClient}
import zio.iae.testkit.{FlociLayer, InMemoryCloud}
import zio.test.*

/** The same tests at two layers of the test onion: an in-memory fake, then Floci. */
object PhotosSpec extends ZIOSpecDefault:
  val photo = ObjectKey.from("photos/cat.jpg").toOption.get
  val note  = ObjectKey.from("notes/todo.txt").toOption.get

  val behaviour = suite("Photos")(
    test("stores, fetches and lists photos"):
      for
        _       <- Photos.store(photo, Chunk.fromArray("meow".getBytes(UTF_8)), "image/jpeg")
        _       <- Photos.store(note, Chunk.fromArray("buy milk".getBytes(UTF_8)), "text/plain")
        fetched <- Photos.fetch(photo)
        catalog <- Photos.catalog
      yield assertTrue(String(fetched.toArray, UTF_8) == "meow", catalog.map(_.key) == Chunk(photo))
  )

  val inMemory: ZLayer[Any, InfraError, Photos.Env] =
    InMemoryCloud.layer(PhotoApp.appName, EnvName("test")) >>> PhotoApp.infra.layer

  val floci: ZLayer[Any, Any, Photos.Env] =
    (FlociLayer.live ++ Client.default) >>> (S3Client.layer ++ (AwsJsonClient.layer >>> (SqsClient.layer ++ SecretsClient.layer ++ DynamoClient.layer))) >>>
      AwsCloud.layer(PhotoApp.appName, EnvName("test"), ProvisionMode.AutoProvision, BucketNaming(None)) >>>
      PhotoApp.infra.layer

  def spec = suite("photo-app")(
    test("declares one versioned bucket it can read and write"):
      val manifest = PhotoApp.infra.manifest(PhotoApp.appName)
      assertTrue(
        manifest.map(_.resources.map(_.spec.kind)) == Right(List(ResourceKind.Bucket, ResourceKind.Role)),
        manifest.map(_.resources.head) ==
          Right(ResourceDecl(LogicalId("uploads"), ResourceSpec.Bucket(BucketSpec(versioning = Versioning.Enabled)))),
        manifest.map(_.grants.map(_.capability)) == Right(List(Capability.BucketRead, Capability.BucketWrite)),
      )
    ,
    suite("in memory")(behaviour).provideLayer(inMemory),
    suite("on Floci")(behaviour).provideLayerShared(floci) @@ TestAspect.withLiveClock,
  )
