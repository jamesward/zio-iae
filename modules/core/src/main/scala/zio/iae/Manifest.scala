package zio.iae

import zio.json.*
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** The desired state of an app at a commit: what the engine reconciles against the cloud. */
final case class Manifest(app: AppName, resources: List[ResourceDecl], grants: List[Grant]) derives Schema, CanEqual:
  def toJson: String = Manifest.codec.encoder.encodeJson(this, Some(0)).toString

object Manifest:
  /** The logical id of the role an app runs as, derived from its grants. */
  val runtimeRole: LogicalId = LogicalId("runtime")

  given codec: zio.json.JsonCodec[Manifest] = JsonCodec.jsonCodec(Schema[Manifest])

  def fromJson(json: String): Either[String, Manifest] = json.fromJson[Manifest]
