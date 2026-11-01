package zio.iae.testkit

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.iae.*
import zio.stream.ZStream
import zio.test.*

object InMemoryCloudSpec extends ZIOSpecDefault:
  object Uploads extends Bucket.Id(LogicalId("uploads"))
  object Thumbs  extends Bucket.Id(LogicalId("thumbs"))

  def key(value: String): ObjectKey = ObjectKey.from(value).toOption.get

  val infra = Bucket.readWrite(Uploads) ++ Bucket.readWrite(Thumbs)

  def spec = suite("InMemoryCloud")(
    test("each declared bucket is its own service with its own objects"):
      val program =
        for
          uploads <- ZIO.service[Bucket[Uploads.type]]
          thumbs  <- ZIO.service[Bucket[Thumbs.type]]
          _       <- uploads.put(key("a"), ZStream.fromChunk(Chunk.fromArray("full".getBytes(UTF_8))))
          _       <- thumbs.put(key("a"), ZStream.fromChunk(Chunk.fromArray("tiny".getBytes(UTF_8))))
          full    <- uploads.get(key("a")).runCollect
          tiny    <- thumbs.get(key("a")).runCollect
          missing <- thumbs.get(key("b")).runCollect.either
        yield assertTrue(
          String(full.toArray, UTF_8) == "full",
          String(tiny.toArray, UTF_8) == "tiny",
          missing == Left(BucketError.NotFound(key("b"))),
        )
      program.provide(InMemoryCloud.layer(AppName("photo-app"), EnvName("test")) >>> infra.layer)
    ,
  )

object InMemorySecretSpec extends ZIOSpecDefault:
  object ApiKey extends Secret.Id(LogicalId("api-key"))

  def spec = suite("InMemoryCloud secrets")(
    test("a secret has no value until one is set"):
      val program =
        for
          cloud  <- ZIO.service[Cloud]
          reader <- cloud.secret[ApiKey.type](ApiKey.logicalId, ApiKey.spec)
          before <- reader.read.either
          _      <- cloud match
                      case memory: InMemoryCloud => memory.setSecret(ApiKey.logicalId, "s3cr3t")
                      case _                     => ZIO.unit
          after  <- reader.read
        yield assertTrue(before == Left(SecretError.NotSet(ApiKey.logicalId)), after.value == "s3cr3t")
      program.provide(InMemoryCloud.layer(AppName("photo-app"), EnvName("test")))
  )
