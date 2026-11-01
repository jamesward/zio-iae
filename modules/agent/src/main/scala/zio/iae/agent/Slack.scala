package zio.iae.agent

import java.time.Instant
import zio.*
import zio.iae.engine.policy.SlackGroup
import zio.schema.{Schema, derived}

/** What an approval request covers, carried in the Slack message's metadata so the agent can find and check it later
  * without keeping any state of its own.
  */
final case class ApprovalRequest(
    app: String,
    env: String,
    manifest: String,
    plan: String,
    changes: List[String],
    count: Int,
    groups: List[String],
    author: Option[String],
    expires: Long,
) derives Schema, CanEqual

final case class Reaction(name: String, users: List[String]) derives CanEqual

final case class SlackMessage(
    ts: String,
    user: Option[String],
    botId: Option[String],
    text: String,
    request: Option[ApprovalRequest],
    reactions: List[Reaction],
) derives CanEqual:
  def postedAt: Instant = Instant.ofEpochSecond(ts.takeWhile(_ != '.').toLongOption.getOrElse(0L)).nn

/** Who the agent is in Slack: its bot id (messages it posts carry it) and its user id. */
final case class SlackIdentity(botId: String, userId: String)

/** The Slack Web API calls the agent makes. */
trait SlackApi:
  def identity: IO[AgentError, SlackIdentity]
  def post(channel: String, text: String, thread: Option[String], request: Option[ApprovalRequest]): IO[AgentError, String]
  def update(channel: String, ts: String, text: String): IO[AgentError, Unit]
  def history(channel: String, since: Instant): IO[AgentError, List[SlackMessage]]
  def groupMembers(group: SlackGroup): IO[AgentError, Set[String]]
  def isBot(user: String): IO[AgentError, Boolean]
