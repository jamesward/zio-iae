package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import zio.*
import zio.iae.aws.client.*
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** Where an env's evidence goes: its evidence bucket and its Audit Manager assessment, with the id of each IAE
  * control (`IAE-CM-1` ...) in the assessment's control set. `iae bootstrap` creates them.
  */
final case class AuditSettings(bucket: String, assessment: String, controlSet: String, controls: Map[String, String]) derives Schema

final case class ApprovalEvidence(channel: String, requestTs: String, approvers: List[String]) derives Schema

/** What happened in one apply, as recorded for audit. */
final case class ChangeEvidence(
    app: String,
    env: String,
    commit: String,
    authorEmail: String,
    author: Option[String],
    policyCommit: Option[String],
    plan: String,
    phase: String,
    changes: List[String],
    decision: String,
    reasons: List[String],
    approval: Option[ApprovalEvidence],
    applied: List[String],
    at: Instant,
) derives Schema

final case class EvidenceDocument(control: String, statement: String, change: ChangeEvidence) derives Schema

/** Writes an apply's evidence documents to the env's evidence bucket and imports them into the assessment, one
  * document per control. Evidence is about something that already happened, so failures are reported, not retried
  * into the apply.
  */
final case class EvidenceRecorder(s3: S3Client, auditManager: AuditManagerClient, settings: AuditSettings):
  import EvidenceRecorder.*

  def record(change: ChangeEvidence): IO[String, Unit] =
    for
      bucket <- ZIO.fromEither(BucketName.from(settings.bucket))
      _ <- ZIO.foreachDiscard(statements.toList.sortBy(_._1).flatMap((c, s) => settings.controls.get(c).map((c, s, _)))): (control, statement, controlId) =>
             val key  = S3Key.from(s"iae/evidence/${change.env}/${change.app}/${change.commit}/${change.plan.take(12)}-${change.phase}/$control.json").toOption.get
             val json = encoder.encodeJson(EvidenceDocument(control, statement, change), None).toString
             for
               _      <- s3.putObject(bucket, key, Chunk.fromArray(json.getBytes(UTF_8)), Some("application/json")).mapError(e => s"writing $control evidence: $e")
               errors <- auditManager.importEvidence(settings.assessment, settings.controlSet, controlId, List(s"s3://${bucket.value}/${key.value}"))
                           .mapError(e => s"importing $control evidence: ${e.message}")
               _      <- ZIO.fail(s"importing $control evidence: ${errors.mkString("; ")}").when(errors.nonEmpty)
             yield ()
    yield ()

object EvidenceRecorder:
  /** The controls the agent's applies give evidence for. Tests (CM-2), verification (CM-4) and exceptions (CM-5)
    * come from builds, deploys and break-glass.
    */
  val statements: Map[String, String] = Map(
    "IAE-CM-1" -> "The change originates from a commit on the deployed branch.",
    "IAE-CM-3" -> "The plan was approved as the policy deployed in the env requires.",
  )

  private val encoder = JsonCodec.jsonEncoder(Schema[EvidenceDocument])
