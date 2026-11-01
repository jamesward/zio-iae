package zio.iae.testkit

import java.time.LocalDate
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** Audit Manager's manual evidence import, which Floci doesn't emulate: records what is imported per control and
  * enforces the daily limit of 100 imports per control. Point an `AwsConfig`'s endpoint at `url`.
  */
final case class AuditManagerFake(url: URL, private val imported: Ref[Map[(String, String, String), Chunk[(LocalDate, String)]]]):
  /** The `s3://` paths imported into a control. */
  def evidence(assessmentId: String, controlSetId: String, controlId: String): UIO[Chunk[String]] =
    imported.get.map(_.getOrElse((assessmentId, controlSetId, controlId), Chunk.empty).map(_._2))

  def all: UIO[Map[(String, String, String), Chunk[String]]] = imported.get.map(_.view.mapValues(_.map(_._2)).toMap)

object AuditManagerFake:
  val dailyLimit = 100

  val make: ZIO[Scope, Throwable, AuditManagerFake] =
    for
      imported <- Ref.make(Map.empty[(String, String, String), Chunk[(LocalDate, String)]])
      server   <- Server.defaultWithPort(0).build
      port     <- Server.install(routes(imported)).provideEnvironment(server)
      url      <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield AuditManagerFake(url, imported)

  private def routes(imported: Ref[Map[(String, String, String), Chunk[(LocalDate, String)]]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / "assessments" / string("assessment") / "controlSets" / string("controlSet") / "controls" / string("control") / "evidence" ->
        handler: (assessment: String, controlSet: String, control: String, req: Request) =>
          for
            body  <- req.body.asString.orElseSucceed("")
            today <- Clock.localDateTime.map(_.toLocalDate.nn)
            paths  = body.fromJson[Json.Obj].toOption.flatMap(_.get("manualEvidence")).flatMap(_.asArray).getOrElse(Chunk.empty)
                       .flatMap(_.asObject.flatMap(_.get("s3ResourcePath")).flatMap(_.asString))
            key    = (decode(assessment), decode(controlSet), decode(control))
            result <- imported.modify: all =>
                        val existing = all.getOrElse(key, Chunk.empty)
                        val room     = (dailyLimit - existing.count(_._1.isEqual(today))).max(0)
                        val (ok, over) = paths.splitAt(room)
                        (over, all.updated(key, existing ++ ok.map(today -> _)))
          yield Response.json(Json.Obj("errors" -> Json.Arr(result.map(p =>
            Json.Obj(
              "errorCode"      -> Json.Str("LimitExceeded"),
              "errorMessage"   -> Json.Str(s"more than $dailyLimit manual evidence imports for this control today"),
              "manualEvidence" -> Json.Obj("s3ResourcePath" -> Json.Str(p)),
            )
          )*)).toJson),
    )

  private def decode(segment: String): String = java.net.URLDecoder.decode(segment, "UTF-8").nn
