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

object InMemoryTableSpec extends ZIOSpecDefault:
  import zio.schema.{Schema, derived}

  final case class UserId(value: Int) derives Schema
  final case class User(name: String) derives Schema, CanEqual
  object Users extends Table.Id[UserId, User](LogicalId("users"))

  def spec = suite("InMemoryCloud tables")(
    test("put, get and delete typed items"):
      val program =
        for
          users <- ZIO.service[Table[Users.type]]
          _     <- users.put(UserId(1), User("ada"))
          found <- users.get(UserId(1))
          _     <- users.delete(UserId(1))
          gone  <- users.get(UserId(1))
        yield assertTrue(found.contains(User("ada")), gone.isEmpty)
      program.provide(InMemoryCloud.layer(AppName("photo-app"), EnvName("test")) >>> Table.readWrite(Users).layer)
    ,
    test("keys and values must have the declared types"):
      typeCheck("""
        val users: Table[Users.type] = ???
        users.put("1", User("ada"))
      """).map(result => assertTrue(result.isLeft))
    ,
  )
