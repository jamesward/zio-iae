package zio.iae

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum
import zio.schema.codec.JsonCodec

@simpleEnum
enum ResourceKind derives Schema, CanEqual:
  case Bucket, Queue, Secret, Role

@simpleEnum
enum Versioning derives Schema, CanEqual:
  case Enabled, Disabled

final case class BucketSpec(versioning: Versioning = Versioning.Disabled) derives Schema, CanEqual

/** Which compute service may assume an app's runtime role. */
@simpleEnum
enum TrustedService derives Schema, CanEqual:
  case EcsTasks, Lambda

/** An app's runtime identity: derived from its grants, never declared by hand. */
final case class RoleSpec(grants: List[Grant], trust: List[TrustedService]) derives Schema, CanEqual

/** The desired state of one resource. Closed over every kind zio-iae manages. */
enum ResourceSpec derives Schema, CanEqual:
  case Bucket(spec: BucketSpec)
  case Queue(spec: QueueSpec)
  case Secret(spec: SecretSpec)
  case Role(spec: RoleSpec)

  def kind: ResourceKind = this match
    case Bucket(_) => ResourceKind.Bucket
    case Queue(_)  => ResourceKind.Queue
    case Secret(_) => ResourceKind.Secret
    case Role(_)   => ResourceKind.Role

final case class ResourceDecl(id: LogicalId, spec: ResourceSpec) derives Schema, CanEqual:
  def kind: ResourceKind = spec.kind

/** Where a resource lives: one logical resource of one app in one env. */
final case class Address(app: AppName, env: EnvName, kind: ResourceKind, id: LogicalId) derives Schema, CanEqual:
  def show: String = s"${app.value}/${env.value}/$kind/${id.value}"

opaque type SpecHash = String

object SpecHash:
  private val length = 16

  /** Content hash of a spec's canonical JSON. Stored on resources as a tag to detect out-of-date or hand-changed
    * resources.
    */
  def of(spec: ResourceSpec): SpecHash =
    val json   = JsonCodec.jsonEncoder(Schema[ResourceSpec]).encodeJson(spec, None).toString
    val digest = MessageDigest.getInstance("SHA-256").nn.digest(json.getBytes(UTF_8))
    HexFormat.of().nn.formatHex(digest).nn.take(length)

  def from(value: String): Option[SpecHash] = Option.when(value.length == length && value.forall(isHex))(value)

  private def isHex(c: Char): Boolean = c.isDigit || ('a' to 'f').contains(c)

  extension (hash: SpecHash) def value: String = hash

  given CanEqual[SpecHash, SpecHash] = CanEqual.derived
