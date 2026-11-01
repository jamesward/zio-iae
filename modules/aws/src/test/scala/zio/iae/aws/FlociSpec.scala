package zio.iae.aws

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.stream.ZStream
import zio.test.*

/** Integration tests against Floci. Each test uses its own env name, so tests share one container safely. */
object FlociSpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val naming = BucketNaming(None)

  object Uploads extends Bucket.Id(LogicalId("uploads"))
  object Archive extends Bucket.Id(LogicalId("archive"), BucketSpec(versioning = Versioning.Enabled))

  val uniqueEnv: UIO[EnvName] =
    ZIO
      .collectAll(List.fill(8)(Random.nextIntBounded(26).map(i => ('a' + i).toChar)))
      .map(cs => EnvName.from("t" + cs.mkString).toOption.get)

  def key(value: String): ObjectKey = ObjectKey.from(value).toOption.get

  def bytes(text: String): ZStream[Any, Nothing, Byte] = ZStream.fromChunk(Chunk.fromArray(text.getBytes(UTF_8)))

  def cloud(env: EnvName, mode: ProvisionMode): URIO[S3Client, Cloud] =
    ZIO.serviceWith[S3Client](s3 => AwsCloud(app, env, mode, s3, BucketProvider(s3, naming)))

  def spec = suite("Floci")(
    suite("BucketProvider")(
      test("create, then observe, then nothing left to change"):
        for
          env      <- uniqueEnv
          provider <- ZIO.serviceWith[S3Client](BucketProvider(_, naming))
          address   = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
          desired   = BucketSpec(versioning = Versioning.Enabled)
          before   <- provider.observe(address)
          create    = provider.diff(address, desired, before)
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local))
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local)) // re-running is safe
          after    <- provider.observe(address)
        yield assertTrue(
          before.isEmpty,
          create.exists(_.isInstanceOf[Change.Create[?]]),
          after.exists(_.versioning == VersioningStatus.Enabled),
          provider.diff(address, desired, after).isEmpty,
        )
      ,
      test("a spec change converges and keeps tags that aren't ours"):
        for
          env      <- uniqueEnv
          s3       <- ZIO.service[S3Client]
          provider  = BucketProvider(s3, naming)
          address   = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
          _        <- ZIO.foreachDiscard(provider.diff(address, BucketSpec(), None))(provider.applyChange(_, ApplyContext.local))
          name     <- ZIO.fromEither(naming.physical(address))
          tags     <- s3.getBucketTagging(name)
          _        <- s3.putBucketTagging(name, tags + ("team" -> "photos"))
          desired   = BucketSpec(versioning = Versioning.Enabled)
          observed <- provider.observe(address)
          update    = provider.diff(address, desired, observed)
          _        <- ZIO.foreachDiscard(update)(provider.applyChange(_, ApplyContext.local))
          after    <- provider.observe(address)
        yield assertTrue(
          update.exists(_.isInstanceOf[Change.Update[?]]),
          provider.diff(address, desired, after).isEmpty,
          after.exists(_.tags.get("team").contains("photos")),
        )
      ,
    ),
    suite("AwsCloud")(
      test("binding fails until the bucket is provisioned, then binds"):
        for
          env    <- uniqueEnv
          bind   <- cloud(env, ProvisionMode.BindOnly)
          auto   <- cloud(env, ProvisionMode.AutoProvision)
          before <- bind.bucket[Uploads.type](Uploads.logicalId, Uploads.spec).either
          _      <- auto.bucket[Uploads.type](Uploads.logicalId, Uploads.spec)
          after  <- bind.bucket[Uploads.type](Uploads.logicalId, Uploads.spec).either
        yield assertTrue(
          before match
            case Left(InfraError.NotProvisioned(address)) => address.id == Uploads.logicalId
            case _                                        => false
          ,
          after.isRight,
        )
      ,
      test("binding to a bucket provisioned from another spec is a mismatch"):
        for
          env    <- uniqueEnv
          auto   <- cloud(env, ProvisionMode.AutoProvision)
          bind   <- cloud(env, ProvisionMode.BindOnly)
          _      <- auto.bucket[Uploads.type](Uploads.logicalId, BucketSpec())
          result <- bind.bucket[Uploads.type](Uploads.logicalId, BucketSpec(versioning = Versioning.Enabled)).either
        yield assertTrue(result match
          case Left(InfraError.SpecMismatch(_, _, actual)) => actual.isDefined
          case _                                           => false
        )
      ,
    ),
    suite("Bucket capability")(
      test("a program uses the buckets it declares"):
        val infra = Bucket.readWrite(Uploads) ++ Bucket.reader(Archive)
        val program =
          for
            uploads <- ZIO.service[Bucket[Uploads.type]]
            archive <- ZIO.service[BucketReader[Archive.type]]
            _       <- uploads.put(key("photos/a.jpg"), bytes("first"), Some("image/jpeg"))
            _       <- uploads.put(key("photos/b.jpg"), bytes("second"))
            _       <- uploads.put(key("notes/c.txt"), bytes("third"))
            body    <- uploads.get(key("photos/a.jpg")).runCollect.map(c => String(c.toArray, UTF_8))
            meta    <- uploads.head(key("photos/b.jpg"))
            photos  <- uploads.list(Some(key("photos/"))).runCollect
            _       <- uploads.delete(key("photos/a.jpg"))
            missing <- uploads.get(key("photos/a.jpg")).runCollect.either
            gone    <- uploads.head(key("photos/a.jpg"))
            empty   <- archive.list().runCollect
          yield assertTrue(
            body == "first",
            meta.exists(_.size == 6L),
            photos.map(_.key.value).toList == List("photos/a.jpg", "photos/b.jpg"),
            missing == Left(BucketError.NotFound(key("photos/a.jpg"))),
            gone.isEmpty,
            empty.isEmpty,
          )
        for
          env    <- uniqueEnv
          c      <- cloud(env, ProvisionMode.AutoProvision)
          result <- program.provideLayer(ZLayer.succeed(c) >>> infra.layer)
        yield result
      ,
    ),
  ).provideShared(FlociLayer.live, Client.default, S3Client.layer) @@ TestAspect.withLiveRandom @@ TestAspect.withLiveClock
