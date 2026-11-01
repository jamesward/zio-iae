package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*
import zio.stream.ZStream

/** A `Bucket` backed by S3. Reads and writes are limited at runtime by the IAM role generated from the app's grants. */
final case class S3Bucket[I](s3: S3Client, name: BucketName) extends Bucket[I]:
  def get(key: ObjectKey): ZStream[Any, BucketError, Byte] =
    ZStream.fromZIO(s3Key(key)).flatMap(k => s3.getObject(name, k).mapError(toBucketError(Some(key))))

  def head(key: ObjectKey): IO[BucketError, Option[ObjectMeta]] =
    s3Key(key).flatMap(k => s3.headObject(name, k).mapBoth(toBucketError(Some(key)), _.map(meta(key, _))))

  def list(prefix: Option[ObjectKey]): ZStream[Any, BucketError, ObjectMeta] =
    s3.listObjects(name, prefix.map(_.value))
      .mapError(toBucketError(None))
      .mapZIO(o => ZIO.fromEither(ObjectKey.from(o.key.value)).mapBoth(BucketError.Unavailable(_), meta(_, o)))

  def put(key: ObjectKey, body: ZStream[Any, Throwable, Byte], contentType: Option[String]): IO[BucketError, Unit] =
    for
      k     <- s3Key(key)
      bytes <- body.runCollect.mapError(e => BucketError.BodyFailed(e.getMessage))
      _     <- s3.putObject(name, k, bytes, contentType).mapError(toBucketError(Some(key)))
    yield ()

  def delete(key: ObjectKey): IO[BucketError, Unit] =
    s3Key(key).flatMap(k => s3.deleteObject(name, k).mapError(toBucketError(Some(key))))

  private def s3Key(key: ObjectKey): IO[BucketError, S3Key] =
    ZIO.fromEither(S3Key.from(key.value)).mapError(BucketError.Unavailable(_))

  private def meta(key: ObjectKey, o: S3Object): ObjectMeta = ObjectMeta(key, o.size, o.eTag, o.lastModified)

  private def toBucketError(key: Option[ObjectKey])(error: S3Error): BucketError = (error, key) match
    case (S3Error.NoSuchKey(_, _), Some(k)) => BucketError.NotFound(k)
    case (S3Error.AccessDenied(detail), _)  => BucketError.AccessDenied(detail)
    case (other, _)                         => BucketError.Unavailable(other.message)
