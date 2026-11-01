package zio.iae.engine

import zio.iae.CommitSha
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** What the webhook puts on the agent's inbox queue. Only identifiers: the agent re-reads everything else from git,
  * the cloud and Slack, so a forged or replayed event can at most cause an extra reconcile.
  */
enum InboxEvent derives Schema, CanEqual:
  case Pushed(delivery: String, repository: String, branch: String, commit: CommitSha)
  case SlackReaction(channel: String, messageTs: String, reaction: String, user: String)
  case SlackReply(channel: String, threadTs: String, user: String)
  case Tick

object InboxEvent:
  def encode(event: InboxEvent): String = JsonCodec.jsonEncoder(Schema[InboxEvent]).encodeJson(event, None).toString
  def decode(json: String): Either[String, InboxEvent] = JsonCodec.jsonDecoder(Schema[InboxEvent]).decodeJson(json)
