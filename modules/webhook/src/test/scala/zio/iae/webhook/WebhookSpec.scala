package zio.iae.webhook

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.*
import zio.iae.CommitSha
import zio.iae.engine.InboxEvent
import zio.test.*

object WebhookSpec extends ZIOSpecDefault:
  final case class Collecting(events: Ref[Chunk[InboxEvent]]) extends Inbox:
    def send(event: InboxEvent): IO[String, Unit] = events.update(_ :+ event)

  val settings = WebhookSettings(WebhookSecret("gh-secret"), WebhookSecret("slack-secret"))
  val sha      = "a" * 40

  def webhook: UIO[(Webhook, Ref[Chunk[InboxEvent]])] =
    Ref.make(Chunk.empty[InboxEvent]).map(events => (Webhook(settings, Collecting(events)), events))

  def githubRequest(event: String, body: String, secret: WebhookSecret = settings.github): Request =
    Request
      .post(URL.root / "github", Body.fromString(body))
      .addHeaders(
        Headers(
          Header.Custom("x-github-event", event),
          Header.Custom("x-github-delivery", "d-1"),
          Header.Custom("x-hub-signature-256", Signatures.github(secret, body.getBytes(UTF_8))),
        )
      )

  def slackRequest(body: String, timestamp: Long): Request =
    Request
      .post(URL.root / "slack", Body.fromString(body))
      .addHeaders(
        Headers(
          Header.Custom("x-slack-request-timestamp", timestamp.toString),
          Header.Custom("x-slack-signature", Signatures.slack(settings.slack, timestamp.toString, body.getBytes(UTF_8))),
        )
      )

  val push = s"""{"ref":"refs/heads/main","after":"$sha","repository":{"full_name":"acme/photos"},"commits":[]}"""

  def spec = suite("Webhook")(
    test("GitHub signatures match GitHub's documented example"):
      val signature = Signatures.github(WebhookSecret("It's a Secret to Everybody"), "Hello, World!".getBytes(UTF_8))
      assertTrue(signature == "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17")
    ,
    test("a signed push becomes a Pushed event with only identifiers"):
      for
        (hook, events) <- webhook
        response       <- hook.github(githubRequest("push", push))
        sent           <- events.get
      yield assertTrue(
        response.status.code == Status.Accepted.code,
        sent == Chunk(InboxEvent.Pushed("d-1", "acme/photos", "main", CommitSha.from(sha).toOption.get)),
      )
    ,
    test("a wrong signature is rejected and nothing is enqueued"):
      for
        (hook, events) <- webhook
        response       <- hook.github(githubRequest("push", push, WebhookSecret("guess")))
        sent           <- events.get
      yield assertTrue(response.status.code == Status.Unauthorized.code, sent.isEmpty)
    ,
    test("events the agent doesn't act on are accepted and dropped"):
      for
        (hook, events) <- webhook
        response       <- hook.github(githubRequest("issues", """{"action":"opened"}"""))
        tag            <- hook.github(githubRequest("push", push.replace("refs/heads/main", "refs/tags/v1")))
        sent           <- events.get
      yield assertTrue(response.status.code == Status.Accepted.code, tag.status.code == Status.Accepted.code, sent.isEmpty)
    ,
    test("Slack reactions and thread replies become events; bot messages don't"):
      val reaction = """{"type":"event_callback","event":{"type":"reaction_added","user":"U1","reaction":"white_check_mark","item":{"type":"message","channel":"C1","ts":"111.1"}}}"""
      val reply    = """{"type":"event_callback","event":{"type":"message","user":"U2","channel":"C1","thread_ts":"111.1","text":"use a new bucket"}}"""
      val bot      = """{"type":"event_callback","event":{"type":"message","bot_id":"B1","user":"U3","channel":"C1","thread_ts":"111.1"}}"""
      for
        (hook, events) <- webhook
        now            <- Clock.instant.map(_.getEpochSecond)
        _              <- ZIO.foreachDiscard(List(reaction, reply, bot))(b => hook.slack(slackRequest(b, now)))
        sent           <- events.get
      yield assertTrue(sent == Chunk(InboxEvent.SlackReaction("C1", "111.1", "white_check_mark", "U1"), InboxEvent.SlackReply("C1", "111.1", "U2")))
    ,
    test("a mention at the top of the channel becomes a mention; one in a thread is left to the thread's message event"):
      val top      = """{"type":"event_callback","event":{"type":"app_mention","user":"U4","channel":"C1","ts":"222.2","text":"<@U_IAE> status?"}}"""
      val inThread = """{"type":"event_callback","event":{"type":"app_mention","user":"U4","channel":"C1","ts":"333.3","thread_ts":"111.1","text":"<@U_IAE> and?"}}"""
      for
        (hook, events) <- webhook
        now            <- Clock.instant.map(_.getEpochSecond)
        _              <- ZIO.foreachDiscard(List(top, inThread))(b => hook.slack(slackRequest(b, now)))
        sent           <- events.get
      yield assertTrue(sent == Chunk(InboxEvent.SlackMention("C1", "222.2", "U4")))
    ,
    test("Slack requests that are stale or unsigned are rejected; URL verification is answered"):
      val challenge = """{"type":"url_verification","challenge":"xyz"}"""
      for
        (hook, events) <- webhook
        now            <- Clock.instant.map(_.getEpochSecond)
        stale          <- hook.slack(slackRequest(challenge, now - 600))
        answered       <- hook.slack(slackRequest(challenge, now))
        body           <- answered.body.asString
        unsigned       <- hook.slack(Request.post(URL.root / "slack", Body.fromString(challenge)))
      yield assertTrue(stale.status.code == Status.Unauthorized.code, body == "xyz", unsigned.status.code == Status.Unauthorized.code)
    ,
  ) @@ TestAspect.withLiveClock
