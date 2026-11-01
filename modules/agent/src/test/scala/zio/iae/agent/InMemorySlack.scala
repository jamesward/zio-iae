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
    threads: Ref[Map[(String, String), List[SlackMessage]]],
) extends SlackApi:
  def identity: IO[AgentError, SlackIdentity] = ZIO.succeed(self)

  def post(channel: String, text: String, thread: Option[String], request: Option[ApprovalRequest]): IO[AgentError, String] =
    for
      now <- Clock.instant
      seq <- clock.updateAndGet(_ + 1)
      ts   = s"${now.getEpochSecond}.${f"$seq%06d"}"
      _ <- thread match
             case Some(t) => threads.update(m => m.updated((channel, t), m.getOrElse((channel, t), Nil) :+ SlackMessage(ts, Some(self.userId), Some(self.botId), text, None, Nil)))
             case None    => messages.update(_ :+ (channel -> SlackMessage(ts, Some(self.userId), Some(self.botId), text, request, Nil)))
    yield ts

  def update(channel: String, ts: String, text: String, request: Option[ApprovalRequest]): IO[AgentError, Unit] =
    messages.update(_.map((c, m) => if c == channel && m.ts == ts then c -> m.copy(text = text, request = request.orElse(m.request)) else c -> m))

  def history(channel: String, since: Instant): IO[AgentError, List[SlackMessage]] =
    messages.get.map(_.collect { case (`channel`, m) if !m.postedAt.isBefore(since.minusSeconds(1)) => m }.toList.reverse)

  def groupMembers(group: SlackGroup): IO[AgentError, Set[String]] = ZIO.succeed(groups.getOrElse(group.handle, Set.empty))

  def groupId(group: SlackGroup): IO[AgentError, Option[String]] = ZIO.succeed(Option.when(groups.contains(group.handle))(s"S_${group.handle}"))

  /** The texts of a thread's replies. */
  def threadTexts(channel: String, thread: String): UIO[List[String]] = threads.get.map(_.getOrElse((channel, thread), Nil).map(_.text))

  def replies(channel: String, thread: String): IO[AgentError, List[SlackMessage]] =
    for
      root <- messages.get.map(_.collectFirst { case (`channel`, m) if m.ts == thread => m })
      rest <- threads.get.map(_.getOrElse((channel, thread), Nil))
    yield root.toList ++ rest

  /** A person posting: in a thread, or at the top of the channel. Returns the message's ts. */
  def say(channel: String, user: String, text: String, thread: Option[String] = None): UIO[String] =
    for
      now <- Clock.instant
      seq <- clock.updateAndGet(_ + 1)
      ts   = s"${now.getEpochSecond}.${f"$seq%06d"}"
      m    = SlackMessage(ts, Some(user), None, text, None, Nil)
      _ <- thread match
             case Some(t) => threads.update(all => all.updated((channel, t), all.getOrElse((channel, t), Nil) :+ m))
             case None    => messages.update(_ :+ (channel -> m))
    yield ts

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
      threads  <- Ref.make(Map.empty[(String, String), List[SlackMessage]])
    yield InMemorySlack(SlackIdentity("B_IAE", "U_IAE"), messages, groups, bots, clock, threads)
