package zio.iae.testkit

import java.security.MessageDigest
import java.time.Instant
import java.util.HexFormat
import zio.*
import zio.iae.*
import zio.stream.ZStream

/** A `Cloud` whose buckets live in memory: the fast, fake layer of the test onion. */
final case class InMemoryCloud(
    app: AppName,
    env: EnvName,
    buckets: Ref[Map[LogicalId, Map[ObjectKey, InMemoryCloud.Stored]]],
    queues: Ref.Synchronized[Map[LogicalId, Ref[InMemoryQueue.State]]],
) extends Cloud:
  def bucket[I](id: LogicalId, spec: BucketSpec): IO[InfraError, Bucket[I]] =
    buckets.update(all => if all.contains(id) then all else all + (id -> Map.empty)).as(InMemoryBucket[I](id, buckets))

  def queue[I <: MessageQueue.Id[?]](id: I): IO[InfraError, MessageQueue[I]] =
    queues
      .modifyZIO: all =>
        all.get(id.logicalId) match
          case Some(state) => ZIO.succeed((state, all))
          case None        => Ref.make(InMemoryQueue.empty).map(state => (state, all + (id.logicalId -> state)))
      .map(InMemoryQueue(id, _))

object InMemoryCloud:
  final case class Stored(bytes: Chunk[Byte], contentType: Option[String], eTag: String, lastModified: Instant)

  def layer(app: AppName, env: EnvName): ULayer[Cloud] =
    ZLayer:
      for
        buckets <- Ref.make(Map.empty[LogicalId, Map[ObjectKey, Stored]])
        queues  <- Ref.Synchronized.make(Map.empty[LogicalId, Ref[InMemoryQueue.State]])
      yield InMemoryCloud(app, env, buckets, queues)

private final case class InMemoryBucket[I](id: LogicalId, buckets: Ref[Map[LogicalId, Map[ObjectKey, InMemoryCloud.Stored]]])
    extends Bucket[I]:
  import InMemoryCloud.Stored

  private def objects: UIO[Map[ObjectKey, Stored]] = buckets.get.map(_.getOrElse(id, Map.empty))

  def get(key: ObjectKey): ZStream[Any, BucketError, Byte] =
    ZStream.fromIterableZIO(objects.flatMap(o => ZIO.fromOption(o.get(key)).orElseFail(BucketError.NotFound(key))).map(_.bytes))

  def head(key: ObjectKey): IO[BucketError, Option[ObjectMeta]] =
    objects.map(_.get(key).map(s => ObjectMeta(key, s.bytes.size.toLong, s.eTag, Some(s.lastModified))))

  def list(prefix: Option[ObjectKey]): ZStream[Any, BucketError, ObjectMeta] =
    ZStream.fromIterableZIO:
      objects.map: o =>
        o.toList
          .filter((k, _) => prefix.forall(p => k.value.startsWith(p.value)))
          .sortBy(_._1.value)
          .map((k, s) => ObjectMeta(k, s.bytes.size.toLong, s.eTag, Some(s.lastModified)))

  def put(key: ObjectKey, body: ZStream[Any, Throwable, Byte], contentType: Option[String]): IO[BucketError, Unit] =
    for
      bytes <- body.runCollect.mapError(e => BucketError.BodyFailed(e.getMessage))
      now   <- Clock.instant
      stored = Stored(bytes, contentType, HexFormat.of().nn.formatHex(MessageDigest.getInstance("MD5").nn.digest(bytes.toArray)).nn, now)
      _     <- buckets.update(all => all.updated(id, all.getOrElse(id, Map.empty).updated(key, stored)))
    yield ()

  def delete(key: ObjectKey): IO[BucketError, Unit] =
    buckets.update(all => all.updated(id, all.getOrElse(id, Map.empty) - key))
