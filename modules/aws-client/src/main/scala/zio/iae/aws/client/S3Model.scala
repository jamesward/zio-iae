package zio.iae.aws.client

import java.time.Instant

opaque type BucketName = String

object BucketName:
  private val pattern = "[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]".r

  def from(value: String): Either[String, BucketName] =
    if !pattern.matches(value) then
      Left(s"'$value' is not a valid S3 bucket name (3-63 lowercase letters, digits, '.' or '-')")
    else if value.contains("..") then Left(s"'$value' must not contain '..'")
    else Right(value)

  extension (name: BucketName) def value: String = name

  given CanEqual[BucketName, BucketName] = CanEqual.derived

opaque type S3Key = String

object S3Key:
  def from(value: String): Either[String, S3Key] = Either.cond(value.nonEmpty, value, "S3 key must not be empty")

  extension (key: S3Key) def value: String = key

  given CanEqual[S3Key, S3Key] = CanEqual.derived

final case class S3Object(key: S3Key, size: Long, eTag: String, lastModified: Option[Instant])

/** A bucket's versioning state as S3 reports it. `Off` means versioning was never enabled. */
enum VersioningStatus derives CanEqual:
  case Off, Enabled, Suspended

enum S3Error derives CanEqual:
  case NoSuchBucket(bucket: BucketName)
  case NoSuchKey(bucket: BucketName, key: S3Key)
  case BucketAlreadyOwnedByYou(bucket: BucketName)
  case BucketNotEmpty(bucket: BucketName)
  case AccessDenied(detail: String)
  case Service(status: Int, code: Option[String], detail: Option[String])
  case Decode(detail: String)
  case InvalidRequest(detail: String)
  case Transport(cause: Throwable)

  def message: String = this match
    case NoSuchBucket(bucket)            => s"bucket ${bucket.value} does not exist"
    case NoSuchKey(bucket, key)          => s"s3://${bucket.value}/${key.value} does not exist"
    case BucketAlreadyOwnedByYou(bucket) => s"bucket ${bucket.value} already exists"
    case BucketNotEmpty(bucket)          => s"bucket ${bucket.value} is not empty"
    case AccessDenied(detail)            => s"access denied: $detail"
    case Service(status, code, detail)   => s"S3 returned $status ${code.getOrElse("")}: ${detail.getOrElse("")}".trim
    case Decode(detail)                  => s"could not read S3 response: $detail"
    case InvalidRequest(detail)          => s"could not build S3 request: $detail"
    case Transport(cause)                => s"request to S3 failed: ${cause.getMessage}"

  /** Throttling, server errors and network failures are worth retrying. */
  def retryable: Boolean = this match
    case Service(status, code, _) => status >= 500 || code.exists(Set("SlowDown", "Throttling", "RequestTimeout"))
    case Transport(_)             => true
    case _                        => false

/** The lifecycle rules iae manages on a bucket, in days, and whether there are rules of anyone else's. */
final case class S3Lifecycle(tieringAfterDays: Option[Int], expireAfterDays: Option[Int], noncurrentExpireAfterDays: Option[Int], others: Boolean) derives CanEqual
