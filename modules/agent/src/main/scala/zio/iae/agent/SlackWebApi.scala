package zio.iae.agent

import java.time.Instant
import zio.*
import zio.http.*
import zio.iae.engine.policy.SlackGroup
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.schema.codec.JsonCodec
import zio.json.*
import zio.json.ast.Json

final case class SlackToken(value: String):
  override def toString: String = "SlackToken(***)"

/** The Slack Web API over HTTP. `base` is `https://slack.com/api` in production and the fake in the local harness. */
final case class SlackWebApi(http: Client, token: SlackToken, base: URL) extends SlackApi:
  import SlackWebApi.*

  def identity: IO[AgentError, SlackIdentity] =
    form[AuthTest]("auth.test", Nil).flatMap(a => ZIO.fromOption(a.botId.zip(a.userId)).mapBoth(_ => AgentError.Slack("auth.test returned no bot"), SlackIdentity(_, _)))

  def post(channel: String, text: String, thread: Option[String], request: Option[ApprovalRequest]): IO[AgentError, String] =
    json[PostMessage, Posted]("chat.postMessage", PostMessage(channel, text, thread, request.map(Metadata(requestType, _)))).map(_.ts)

  def update(channel: String, ts: String, text: String): IO[AgentError, Unit] =
    json[UpdateMessage, Ok]("chat.update", UpdateMessage(channel, ts, text)).unit

  /** Other apps' message metadata can have any shape, so each message is read on its own (as JSON, at this boundary)
    * and only our requests' payloads are decoded.
    */
  def history(channel: String, since: Instant): IO[AgentError, List[SlackMessage]] =
    val params = List("channel" -> channel, "oldest" -> since.getEpochSecond.toString, "limit" -> "200", "include_all_metadata" -> "true")
    for
      text     <- call("conversations.history", formBody(params), formType)
      messages <- ZIO
                    .fromEither(text.fromJson[Json].flatMap(_.asObject.flatMap(_.get("messages")).flatMap(_.asArray).toRight("no messages")))
                    .mapError(e => AgentError.Slack(s"conversations.history: $e"))
      decoded <- ZIO.foreach(messages.toList): raw =>
                   val json = raw.toJson
                   ZIO
                     .fromEither(JsonCodec.jsonDecoder(Schema[MessageJson]).decodeJson(json))
                     .mapError(e => AgentError.Slack(s"conversations.history: $e"))
                     .map: m =>
                       val request = Option
                         .when(m.metadata.exists(_.eventType == requestType))(JsonCodec.jsonDecoder(Schema[RequestMessage]).decodeJson(json).toOption)
                         .flatten
                         .map(_.metadata.eventPayload)
                       SlackMessage(m.ts, m.user, m.botId, m.text.getOrElse(""), request, m.reactions.getOrElse(Nil).map(r => Reaction(r.name, r.users)))
    yield decoded

  def groupMembers(group: SlackGroup): IO[AgentError, Set[String]] =
    form[UserGroups]("usergroups.list", List("include_users" -> "true"))
      .map(_.usergroups.find(_.handle == group.handle).fold(Set.empty[String])(_.users.getOrElse(Nil).toSet))

  def isBot(user: String): IO[AgentError, Boolean] =
    form[UserInfo]("users.info", List("user" -> user)).map(_.user.isBot)

  private def json[Req: Schema, Res: Schema](method: String, body: Req): IO[AgentError, Res] =
    call(method, JsonCodec.jsonEncoder(Schema[Req]).encodeJson(body, None).toString, "application/json; charset=utf-8").flatMap(decode[Res](method))

  private def form[Res: Schema](method: String, params: List[(String, String)]): IO[AgentError, Res] =
    call(method, formBody(params), formType).flatMap(decode[Res](method))

  private def decode[Res: Schema](method: String)(text: String): IO[AgentError, Res] =
    ZIO.fromEither(JsonCodec.jsonDecoder(Schema[Res]).decodeJson(text)).mapError(e => AgentError.Slack(s"$method: $e"))

  /** Calls a method and returns the response body once Slack says `ok`. */
  private def call(method: String, body: String, contentType: String): IO[AgentError, String] =
    val request = Request
      .post(base / method, Body.fromString(body))
      .addHeaders(Headers(Header.Custom("authorization", s"Bearer ${token.value}"), Header.Custom("content-type", contentType)))
    for
      response <- http.batched(request).mapError(e => AgentError.Slack(s"$method: ${e.getMessage}"))
      text     <- response.body.asString.mapError(e => AgentError.Slack(s"$method: ${e.getMessage}"))
      status   <- decode[Ok](method)(text)
      _        <- ZIO.fail(AgentError.Slack(s"$method: ${status.error.getOrElse("failed")}")).unless(status.ok)
    yield text

object SlackWebApi:
  val requestType = "iae_approval_request"

  val production: URL = URL.decode("https://slack.com/api").toOption.get

  final case class Ok(ok: Boolean, error: Option[String]) derives Schema
  final case class AuthTest(@fieldName("user_id") userId: Option[String], @fieldName("bot_id") botId: Option[String]) derives Schema
  final case class Metadata(@fieldName("event_type") eventType: String, @fieldName("event_payload") eventPayload: ApprovalRequest) derives Schema
  final case class PostMessage(channel: String, text: String, @fieldName("thread_ts") threadTs: Option[String], metadata: Option[Metadata]) derives Schema
  final case class Posted(ts: String) derives Schema
  final case class UpdateMessage(channel: String, ts: String, text: String) derives Schema
  final case class ReactionJson(name: String, users: List[String]) derives Schema
  final case class MetadataType(@fieldName("event_type") eventType: String) derives Schema
  final case class MessageJson(
      ts: String,
      user: Option[String],
      @fieldName("bot_id") botId: Option[String],
      text: Option[String],
      metadata: Option[MetadataType],
      reactions: Option[List[ReactionJson]],
  ) derives Schema
  final case class RequestMessage(metadata: Metadata) derives Schema

  private val formType = "application/x-www-form-urlencoded"

  private def formBody(params: List[(String, String)]): String =
    params.map((k, v) => s"${java.net.URLEncoder.encode(k, "UTF-8")}=${java.net.URLEncoder.encode(v, "UTF-8")}").mkString("&")
  final case class UserGroup(id: String, handle: String, users: Option[List[String]]) derives Schema
  final case class UserGroups(usergroups: List[UserGroup]) derives Schema
  final case class User(id: String, @fieldName("is_bot") isBot: Boolean) derives Schema
  final case class UserInfo(user: User) derives Schema
