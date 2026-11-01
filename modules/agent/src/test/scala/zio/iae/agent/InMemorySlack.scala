package zio.iae.agent

import java.time.Instant
import zio.*
import zio.iae.engine.policy.SlackGroup

/** Slack in memory: channels of messages with reactions, user groups and bot users. */
final case class InMemorySlack(
    self: SlackIdentity,
    messages: Ref[Vector[(String, SlackMessage)]],
    groups: Map[String, Set[String]],
    bots: Set[String],
    clock: Ref[Long],
) extends SlackApi:
  def identity: IO[AgentError, SlackIdentity] = ZIO.succeed(self)

  def post(channel: String, text: String, thread: Option[String], request: Option[ApprovalRequest]): IO[AgentError, String] =
    for
      now <- Clock.instant
      seq <- clock.updateAndGet(_ + 1)
      ts   = s"${now.getEpochSecond}.${f"$seq%06d"}"
      _   <- messages.update(_ :+ (channel -> SlackMessage(ts, Some(self.userId), Some(self.botId), text, request, Nil)))
    yield ts

  def update(channel: String, ts: String, text: String): IO[AgentError, Unit] =
    messages.update(_.map((c, m) => if c == channel && m.ts == ts then c -> m.copy(text = text) else c -> m))

  def history(channel: String, since: Instant): IO[AgentError, List[SlackMessage]] =
    messages.get.map(_.collect { case (`channel`, m) if !m.postedAt.isBefore(since.minusSeconds(1)) => m }.toList.reverse)

  def groupMembers(group: SlackGroup): IO[AgentError, Set[String]] = ZIO.succeed(groups.getOrElse(group.handle, Set.empty))

  def isBot(user: String): IO[AgentError, Boolean] = ZIO.succeed(bots.contains(user) || user == self.userId)

  /** A person reacting to a message. */
  def react(ts: String, user: String, reaction: String): UIO[Unit] =
    messages.update(_.map: (c, m) =>
      if m.ts != ts then c -> m
      else
        val others  = m.reactions.filterNot(_.name == reaction)
        val current = m.reactions.find(_.name == reaction).fold(List.empty[String])(_.users)
        c -> m.copy(reactions = others :+ Reaction(reaction, (current :+ user).distinct)))

  def posted(channel: String): UIO[List[SlackMessage]] = messages.get.map(_.collect { case (`channel`, m) => m }.toList)

object InMemorySlack:
  def make(groups: Map[String, Set[String]], bots: Set[String] = Set.empty): UIO[InMemorySlack] =
    for
      messages <- Ref.make(Vector.empty[(String, SlackMessage)])
      clock    <- Ref.make(0L)
    yield InMemorySlack(SlackIdentity("B_IAE", "U_IAE"), messages, groups, bots, clock)
