package zio.iae.engine

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.iae.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum
import zio.schema.codec.JsonCodec

/** Expand adds and updates (safe before a deploy); contract orphans what is no longer declared (after it). */
@simpleEnum
enum Phase derives Schema, CanEqual:
  case Expand, Contract

opaque type ContentHash = String

object ContentHash:
  def of[A: Schema](value: A): ContentHash =
    val json = JsonCodec.jsonEncoder(Schema[A]).encodeJson(value, None).toString
    HexFormat.of().nn.formatHex(MessageDigest.getInstance("SHA-256").nn.digest(json.getBytes(UTF_8))).nn

  extension (hash: ContentHash)
    def value: String = hash
    def short: String = hash.take(12)

  given CanEqual[ContentHash, ContentHash] = CanEqual.derived
  given Schema[ContentHash]                = Schema[String].transform(identity, identity)

/** The changes that take one env from what the cloud reports to what the manifest declares. Content-addressed: an
  * approval names a plan by its hash.
  */
final case class Plan(
    app: AppName,
    env: EnvName,
    target: Option[CommitSha],
    manifest: ContentHash,
    phase: Phase,
    changes: List[PlannedChange],
) derives Schema, CanEqual:
  def hash: ContentHash         = ContentHash.of(this)
  def isEmpty: Boolean          = changes.isEmpty
  def maxRisk: Option[Risk]     = changes.map(_.risk).maxOption
  def toJson: String            = JsonCodec.jsonEncoder(Schema[Plan]).encodeJson(this, Some(0)).toString
