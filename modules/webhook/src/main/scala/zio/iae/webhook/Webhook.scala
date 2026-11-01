package zio.iae.webhook

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.*
import zio.iae.CommitSha
import zio.iae.aws.client.{QueueUrl, SqsClient}
import zio.iae.engine.InboxEvent
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.schema.codec.JsonCodec

/** Where verified events go: the agent's SQS inbox in production, a local queue in tests. */
trait Inbox:
  def send(event: InboxEvent): IO[String, Unit]

final case class SqsInbox(sqs: SqsClient, url: QueueUrl) extends Inbox:
  def send(event: InboxEvent): IO[String, Unit] = sqs.send(url, InboxEvent.encode(event)).mapBoth(_.message, _ => ())

final case class WebhookSettings(github: WebhookSecret, slack: WebhookSecret, maxSlackSkew: Duration = 5.minutes)

/** The agent's only inbound endpoint. It verifies each request's signature, keeps the few events the agent acts on,
  * reduces them to identifiers and enqueues them. Everything else gets a 2xx so senders don't retry.
  */
final case class Webhook(settings: WebhookSettings, inbox: Inbox):
  import Webhook.*

  val routes: Routes[Any, Nothing] =
    Routes(
      Method.POST / "github" -> handler((req: Request) => github(req)),
      Method.POST / "slack"  -> handler((req: Request) => slack(req)),
    )

  def github(req: Request): UIO[Response] =
    req.body.asArray
      .flatMap: body =>
        val signature = req.rawHeader("x-hub-signature-256")
        if !Signatures.matches(Signatures.github(settings.github, body), signature) then ZIO.succeed(Response.status(Status.Unauthorized))
        else
          (req.rawHeader("x-github-event"), req.rawHeader("x-github-delivery")) match
            case (Some("push"), Some(delivery)) => decode[PushPayload](body).flatMap(p => enqueue(pushed(delivery, p)))
            case (Some(_), Some(_))             => ZIO.succeed(Response.status(Status.Accepted))
            case _                              => ZIO.succeed(Response.status(Status.BadRequest))
      .catchAllCause(c => ZIO.logErrorCause("github webhook failed", c).as(Response.status(Status.InternalServerError)))

  def slack(req: Request): UIO[Response] =
    (for
      body <- req.body.asArray
      now  <- Clock.instant
      timestamp = req.rawHeader("x-slack-request-timestamp").flatMap(_.toLongOption)
      fresh     = timestamp.exists(t => (now.getEpochSecond - t).abs <= settings.maxSlackSkew.toSeconds)
      valid     = timestamp.exists(t => Signatures.matches(Signatures.slack(settings.slack, t.toString, body), req.rawHeader("x-slack-signature")))
      response <- if !fresh || !valid then ZIO.logWarning("rejected an unsigned or stale Slack request").as(Response.status(Status.Unauthorized))
                  else decode[SlackEnvelope](body).flatMap(slackEvent)
    yield response).catchAllCause(c => ZIO.logErrorCause("slack webhook failed", c).as(Response.status(Status.BadRequest)))

  private def slackEvent(envelope: SlackEnvelope): IO[String, Response] =
    envelope.tpe match
      case "url_verification" => ZIO.succeed(Response.text(envelope.challenge.getOrElse("")))
      case "event_callback" =>
        envelope.event match
          case Some(e) if e.tpe == "reaction_added" =>
            val event = for
              user <- e.user
              item <- e.item
              ts   <- item.ts
              ch   <- item.channel
            yield InboxEvent.SlackReaction(ch, ts, e.reaction.getOrElse(""), user)
            event.fold(ZIO.succeed(Response.ok))(enqueue)
          case Some(e) if e.tpe == "message" && e.botId.isEmpty =>
            val event = for
              user   <- e.user
              thread <- e.threadTs
              ch     <- e.channel
            yield InboxEvent.SlackReply(ch, thread, user)
            event.fold(ZIO.succeed(Response.ok))(enqueue)
          case _ => ZIO.succeed(Response.ok)
      case _ => ZIO.succeed(Response.ok)

  private def pushed(delivery: String, push: PushPayload): Either[String, InboxEvent] =
    for
      branch <- Option.when(push.ref.startsWith("refs/heads/"))(push.ref.stripPrefix("refs/heads/")).toRight(s"not a branch: ${push.ref}")
      commit <- CommitSha.from(push.after)
    yield InboxEvent.Pushed(delivery, push.repository.fullName, branch, commit)

  private def enqueue(event: Either[String, InboxEvent]): IO[String, Response] =
    event match
      case Left(ignored) => ZIO.logInfo(s"ignoring event: $ignored").as(Response.status(Status.Accepted))
      case Right(e)      => inbox.send(e).as(Response.status(Status.Accepted))

  private def enqueue(event: InboxEvent): IO[String, Response] = enqueue(Right(event))

object Webhook:
  private def decode[A: Schema](body: Array[Byte]): IO[String, A] =
    ZIO.fromEither(JsonCodec.jsonDecoder(Schema[A]).decodeJson(String(body, UTF_8)))

  final case class Repository(@fieldName("full_name") fullName: String) derives Schema
  final case class PushPayload(ref: String, after: String, repository: Repository) derives Schema

  final case class SlackItem(channel: Option[String], ts: Option[String]) derives Schema
  final case class SlackEvent(
      @fieldName("type") tpe: String,
      user: Option[String],
      reaction: Option[String],
      item: Option[SlackItem],
      channel: Option[String],
      @fieldName("thread_ts") threadTs: Option[String],
      @fieldName("bot_id") botId: Option[String],
  ) derives Schema
  final case class SlackEnvelope(@fieldName("type") tpe: String, challenge: Option[String], event: Option[SlackEvent]) derives Schema
