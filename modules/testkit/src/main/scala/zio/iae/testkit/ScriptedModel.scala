package zio.iae.testkit

import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** A fake Bedrock Runtime that answers Converse calls from a script, so LLM tasks are tested without a model. Point
  * zio-bedrock's custom origin at `url`. Each call takes the next scripted turn; with none left it answers 500, which
  * the caller sees as a model failure. Requests are recorded as JSON for assertions.
  */
final case class ScriptedModel(url: URL, private val script: Queue[ScriptedModel.Turn], private val received: Ref[Chunk[Json]]):
  def reply(turns: ScriptedModel.Turn*): UIO[Unit] = script.offerAll(turns).unit

  def replyText(texts: String*): UIO[Unit] = reply(texts.map(ScriptedModel.Turn.Text(_))*)

  def requests: UIO[Chunk[Json]] = received.get

object ScriptedModel:
  enum Turn:
    /** An assistant message with one text block (structured output is a text block holding JSON). */
    case Text(text: String)
    /** A Bedrock error response, e.g. 429 throttling or 400 validation. */
    case Failure(status: Int, message: String)
    /** A call of one of the request's tools; the caller runs it and sends the result in its next request. */
    case ToolUse(name: String, input: Json)

  /** Starts the server on a free port for the scope's lifetime. */
  val make: ZIO[Scope, Throwable, ScriptedModel] =
    for
      script   <- Queue.unbounded[Turn]
      received <- Ref.make(Chunk.empty[Json])
      server   <- Server.defaultWithPort(0).build
      port     <- Server.install(routes(script, received)).provideEnvironment(server)
      url      <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield ScriptedModel(url, script, received)

  private def routes(script: Queue[Turn], received: Ref[Chunk[Json]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / "model" / string("model") / "converse" -> handler: (_: String, req: Request) =>
        for
          body <- req.body.asString.orElseSucceed("")
          _    <- received.update(_ :+ body.fromJson[Json].getOrElse(Json.Str(body)))
          turn <- script.poll
        yield turn match
          case Some(Turn.Text(text))               => Response.json(converse(text).toJson)
          case Some(Turn.Failure(status, message)) => error(status, message)
          case Some(Turn.ToolUse(name, input))     => Response.json(toolUse(name, input).toJson)
          case None                                => error(500, "ScriptedModel: no scripted turn left"),
    )

  private def error(status: Int, message: String): Response =
    Response(Status.fromInt(status), body = Body.fromString(Json.Obj("message" -> Json.Str(message)).toJson))
      .contentType(MediaType.application.json)

  private def toolUse(name: String, input: Json): Json =
    Json.Obj(
      "output" -> Json.Obj(
        "message" -> Json.Obj(
          "role"    -> Json.Str("assistant"),
          "content" -> Json.Arr(Json.Obj("toolUse" -> Json.Obj("toolUseId" -> Json.Str(s"tool-$name"), "name" -> Json.Str(name), "input" -> input))),
        )
      ),
      "stopReason" -> Json.Str("tool_use"),
      "usage"      -> Json.Obj("inputTokens" -> Json.Num(0), "outputTokens" -> Json.Num(0), "totalTokens" -> Json.Num(0)),
      "metrics"    -> Json.Obj("latencyMs" -> Json.Num(0)),
    )

  private def converse(text: String): Json =
    Json.Obj(
      "output" -> Json.Obj(
        "message" -> Json.Obj("role" -> Json.Str("assistant"), "content" -> Json.Arr(Json.Obj("text" -> Json.Str(text)))),
      ),
      "stopReason" -> Json.Str("end_turn"),
      "usage"      -> Json.Obj("inputTokens" -> Json.Num(0), "outputTokens" -> Json.Num(0), "totalTokens" -> Json.Num(0)),
      "metrics"    -> Json.Obj("latencyMs" -> Json.Num(0)),
    )
