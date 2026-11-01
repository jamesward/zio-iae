package photos

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.iae.*
import zio.iae.aws.AwsInfraApp
import zio.stream.ZStream

object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))

/** The app's logic, written against the capability it needs, not against S3. */
object Photos:
  type Env = Bucket[Uploads.type]

  def store(key: ObjectKey, bytes: Chunk[Byte], contentType: String): ZIO[Env, BucketError, Unit] =
    ZIO.serviceWithZIO[Env](_.put(key, ZStream.fromChunk(bytes), Some(contentType)))

  def fetch(key: ObjectKey): ZIO[Env, BucketError, Chunk[Byte]] =
    ZIO.serviceWithZIO[Env](_.get(key).runCollect)

  def catalog: ZIO[Env, BucketError, Chunk[ObjectMeta]] =
    ZIO.serviceWithZIO[Env](_.list(ObjectKey.from("photos/").toOption).runCollect)

/** `photo-app --iae-manifest` prints the infra this program needs; otherwise it binds to it and runs. */
object PhotoApp extends AwsInfraApp(AppName("photo-app"), Bucket.readWrite(Uploads)):
  def program: ZIO[Photos.Env, Any, Any] =
    for
      key     <- ZIO.fromEither(ObjectKey.from("photos/hello.txt"))
      _       <- Photos.store(key, Chunk.fromArray("hello, infra".getBytes(UTF_8)), "text/plain")
      catalog <- Photos.catalog
      _       <- ZIO.foreachDiscard(catalog)(m => Console.printLine(s"${m.key.value} (${m.size} bytes)"))
    yield ()
