package zio.iae

import java.time.Instant
import zio.*
import zio.stream.ZStream

opaque type ObjectKey = String

object ObjectKey:
  private val maxBytes = 1024

  def from(value: String): Either[String, ObjectKey] =
    if value.isEmpty then Left("object key must not be empty")
    else if value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maxBytes then
      Left(s"object key must be at most $maxBytes bytes")
    else Right(value)

  extension (key: ObjectKey) def value: String = key

  given CanEqual[ObjectKey, ObjectKey] = CanEqual.derived

final case class ObjectMeta(key: ObjectKey, size: Long, eTag: String, lastModified: Option[Instant]) derives CanEqual

enum BucketError derives CanEqual:
  case NotFound(key: ObjectKey)
  case AccessDenied(detail: String)
  case BodyFailed(detail: String)
  case Unavailable(detail: String)

  def message: String = this match
    case NotFound(key)        => s"object '${key.value}' not found"
    case AccessDenied(detail) => s"access denied: $detail"
    case BodyFailed(detail)   => s"reading the request body failed: $detail"
    case Unavailable(detail)  => s"bucket unavailable: $detail"

trait BucketReader[I]:
  def get(key: ObjectKey): ZStream[Any, BucketError, Byte]
  def head(key: ObjectKey): IO[BucketError, Option[ObjectMeta]]
  def list(prefix: Option[ObjectKey] = None): ZStream[Any, BucketError, ObjectMeta]

trait BucketWriter[I]:
  def put(key: ObjectKey, body: ZStream[Any, Throwable, Byte], contentType: Option[String] = None): IO[BucketError, Unit]
  def delete(key: ObjectKey): IO[BucketError, Unit]

trait Bucket[I] extends BucketReader[I], BucketWriter[I]

object Bucket:
  /** Declares a bucket. Extend it with an object: the object's type identifies the bucket in the ZIO environment.
    * {{{
    * object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
    * }}}
    */
  abstract class Id(val logicalId: LogicalId, val spec: BucketSpec = BucketSpec())

  def reader[I <: Id & Singleton](id: I)(using Tag[BucketReader[I]]): Infra[BucketReader[I]] =
    declare(id, Chunk(Capability.BucketRead))

  def readWrite[I <: Id & Singleton](id: I)(using Tag[Bucket[I]]): Infra[Bucket[I]] =
    declare(id, Chunk(Capability.BucketRead, Capability.BucketWrite))

  private def declare[I <: Id & Singleton, R >: Bucket[I]: Tag](id: I, capabilities: Chunk[Capability]): Infra[R] =
    Infra(
      resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Bucket(id.spec))),
      grants = capabilities.map(Grant(id.logicalId, ResourceKind.Bucket, _)),
      layer = ZLayer.fromZIO[Cloud, InfraError, R](ZIO.serviceWithZIO[Cloud](_.bucket[I](id.logicalId, id.spec))),
    )
