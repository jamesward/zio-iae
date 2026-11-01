package zio.iae.agent.harness

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.*
import zio.iae.agent.SlackWebApi.*
import zio.iae.agent.{ApprovalRequest, SlackWebApi}
import zio.iae.webhook.{Signatures, Webhook, WebhookSecret}
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.schema.codec.JsonCodec
import zio.json.*
import zio.json.ast.Json

final case class FakeUser(id: String, name: String, bot: Boolean = false) derives Schema
final case class FakeReply(user: String, text: String, ts: String) derives Schema
final case class FakeMessage(
    ts: String,
    user: String,
    botId: Option[String],
    text: String,
    request: Option[ApprovalRequest],
    reactions: Map[String, List[String]],
    replies: List[FakeReply],
) derives Schema

/** The Slack Web API calls the agent makes, served from memory, plus the actions people take in the harness UI.
  * People's actions become signed Slack events delivered to the webhook, exactly as Slack would send them.
  */
final case class FakeSlack(
    channel: String,
    users: List[FakeUser],
    groups: Map[String, Set[String]],
    messages: Ref[Vector[FakeMessage]],
    counter: Ref[Long],
    webhook: Webhook,
    signingSecret: WebhookSecret,
):
  import FakeSlack.*

  val bot = FakeUser("U_IAE", "iae", bot = true)

  val api: Routes[Any, Nothing] = Routes(
    Method.POST / "api" / string("method") -> handler: (method: String, req: Request) =>
      (for
        body   <- req.body.asString
        result <- method match
                    case "auth.test"             => ok(AuthTest(Some(bot.id), Some(botId)))
                    case "chat.postMessage"      => decode[PostMessage](body).flatMap(post).flatMap(ts => ok(Posted(ts)))
                    case "chat.update"           => decode[UpdateMessage](body).flatMap(u => messages.update(_.map(m => if m.ts == u.ts then m.copy(text = u.text) else m))) *> okEmpty
                    case "conversations.history" => history
                    case "usergroups.list"       => ok(UserGroups(groups.toList.sortBy(_._1).map((h, us) => UserGroup(s"S_$h", h, Some(us.toList.sorted)))))
                    case "users.info"            => ok(UserInfo(User(form(body)("user"), users.exists(u => u.id == form(body)("user") && u.bot) || form(body)("user") == bot.id)))
                    case other                   => ZIO.succeed(Response.json(s"""{"ok":false,"error":"unknown_method $other"}"""))
      yield result)
        .mapError:
          case t: Throwable => t.getMessage
          case s: String    => s
        .catchAll(e => ZIO.succeed(Response.json(Json.Obj("ok" -> Json.Bool(false), "error" -> Json.Str(e)).toJson)))
  )

  /** Someone reacting in the UI: recorded, then sent to the webhook as Slack would. */
  def react(ts: String, user: String, reaction: String): IO[String, Unit] =
    messages.update(_.map(m => if m.ts != ts then m else m.copy(reactions = m.reactions.updated(reaction, (m.reactions.getOrElse(reaction, Nil) :+ user).distinct)))) *>
      deliver(s"""{"type":"event_callback","event":{"type":"reaction_added","user":"$user","reaction":"$reaction","item":{"type":"message","channel":"$channel","ts":"$ts"}}}""")

  def reply(thread: String, user: String, text: String): IO[String, Unit] =
    for
      ts <- nextTs
      _  <- messages.update(_.map(m => if m.ts != thread then m else m.copy(replies = m.replies :+ FakeReply(user, text, ts))))
      _  <- deliver(s"""{"type":"event_callback","event":{"type":"message","user":"$user","channel":"$channel","thread_ts":"$thread","text":${JsonCodec.jsonEncoder(Schema[String]).encodeJson(text, None)}}}""")
    yield ()

  private def deliver(body: String): IO[String, Unit] =
    for
      now      <- Clock.instant.map(_.getEpochSecond.toString)
      request   = Request.post(URL.root / "slack", Body.fromString(body)).addHeaders(
                    Headers(Header.Custom("x-slack-request-timestamp", now), Header.Custom("x-slack-signature", Signatures.slack(signingSecret, now, body.getBytes(UTF_8))))
                  )
      response <- webhook.slack(request)
      _        <- ZIO.fail(s"webhook answered ${response.status.code}").unless(response.status.isSuccess)
    yield ()

  private def post(message: PostMessage): UIO[String] =
    for
      ts <- nextTs
      _ <- message.threadTs match
             case Some(thread) => messages.update(_.map(m => if m.ts != thread then m else m.copy(replies = m.replies :+ FakeReply(bot.id, message.text, ts))))
             case None         => messages.update(_ :+ FakeMessage(ts, bot.id, Some(botId), message.text, message.metadata.map(_.eventPayload), Map.empty, Nil))
    yield ts

  private def history: UIO[Response] =
    messages.get.map: all =>
      val out = all.reverse.map: m =>
        MessageOut(m.ts, m.user, m.botId, m.text, m.request.map(Metadata(SlackWebApi.requestType, _)), m.reactions.toList.map((n, us) => ReactionJson(n, us)))
      Response.json(s"""{"ok":true,"messages":${JsonCodec.jsonEncoder(Schema[List[MessageOut]]).encodeJson(out.toList, None)}}""")

  private def nextTs: UIO[String] =
    for
      now <- Clock.instant
      n   <- counter.updateAndGet(_ + 1)
    yield s"${now.getEpochSecond}.${f"$n%06d"}"

object FakeSlack:
  val botId = "B_IAE"

  final case class MessageOut(
      ts: String,
      user: String,
      @fieldName("bot_id") botId: Option[String],
      text: String,
      metadata: Option[Metadata],
      reactions: List[ReactionJson],
  ) derives Schema

  def make(channel: String, users: List[FakeUser], groups: Map[String, Set[String]], webhook: Webhook, signingSecret: WebhookSecret): UIO[FakeSlack] =
    for
      messages <- Ref.make(Vector.empty[FakeMessage])
      counter  <- Ref.make(0L)
    yield FakeSlack(channel, users, groups, messages, counter, webhook, signingSecret)

  private def decode[A: Schema](body: String): IO[String, A] = ZIO.fromEither(JsonCodec.jsonDecoder(Schema[A]).decodeJson(body))

  /** Slack's responses are the result's fields plus `"ok": true`. */
  private def ok[A: Schema](value: A): IO[String, Response] =
    ZIO
      .fromEither(JsonCodec.jsonEncoder(Schema[A]).toJsonAST(value))
      .flatMap(json => ZIO.fromOption(json.asObject).orElseFail("not an object"))
      .map(obj => Response.json(obj.add("ok", Json.Bool(true)).toJson))

  private val okEmpty: UIO[Response] = ZIO.succeed(Response.json("""{"ok":true}"""))

  private def form(body: String)(name: String): String =
    body.split('&').toList.map(_.split("=", 2)).collectFirst { case Array(k, v) if k == name => java.net.URLDecoder.decode(v, "UTF-8").nn }.getOrElse("")
