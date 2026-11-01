package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.{Client, Method}
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** Audit Manager's manual evidence import (a REST-JSON API). */
final case class AuditManagerClient(http: Client, config: AwsConfig):
  import AuditManagerClient.*

  /** Imports evidence documents already in S3 (`s3://bucket/key`) into an assessment's control. */
  def importEvidence(assessmentId: String, controlSetId: String, controlId: String, s3Paths: List[String]): IO[AwsError, List[String]] =
    val root = AwsHttp.root(config, service)
    val path = s"/assessments/${enc(assessmentId)}/controlSets/${enc(controlSetId)}/controls/${enc(controlId)}/evidence"
    val body = JsonCodec.jsonEncoder(Schema[Import]).encodeJson(Import(s3Paths.map(ManualEvidence(_))), None).toString
    val attempt =
      for
        req  <- AwsHttp.request(config, service, Method.POST, root.copy(path = path), List("content-type" -> "application/json"), Chunk.fromArray(body.getBytes(UTF_8)), contentSha256Header = false, doubleEncodePath = true)
                  .mapError(AwsError.InvalidRequest(_))
        resp <- AwsHttp.send(http, req).mapError(AwsError.Transport(_))
        text <- resp.body.asString.mapError(AwsError.Transport(_))
        _    <- ZIO.fail(AwsError.Service("auditmanager", resp.status.code, resp.rawHeader("x-amzn-errortype"), Some(text.take(300)))).unless(resp.status.isSuccess)
        out  <- ZIO.fromEither(JsonCodec.jsonDecoder(Schema[Imported]).decodeJson(if text.isBlank then "{}" else text)).mapError(AwsError.Decode("auditmanager", _))
      yield out.errors.getOrElse(Nil).map(e => s"${e.manualEvidence.flatMap(_.s3ResourcePath).getOrElse("?")}: ${e.errorCode.getOrElse("")} ${e.errorMessage.getOrElse("")}".trim)
    attempt.retry(AwsHttp.retryPolicy && Schedule.recurWhile[AwsError](_.retryable))

object AuditManagerClient:
  val service: AwsService = AwsService("auditmanager", "auditmanager")

  /** Segments are encoded as SigV4 canonicalises them, so the signature matches the path sent. */
  private def enc(segment: String): String = SigV4.uriEncode(segment, encodeSlash = true)

  final case class ManualEvidence(s3ResourcePath: String) derives Schema
  final case class Import(manualEvidence: List[ManualEvidence]) derives Schema
  final case class EvidenceRef(s3ResourcePath: Option[String]) derives Schema
  final case class ImportError(errorCode: Option[String], errorMessage: Option[String], manualEvidence: Option[EvidenceRef]) derives Schema
  final case class Imported(errors: Option[List[ImportError]]) derives Schema
