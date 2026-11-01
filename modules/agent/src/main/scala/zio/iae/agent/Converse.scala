package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, ToolHandler}
import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.SlackGroup
import zio.schema.{Schema, derived}

/** The read-only tools of the `Converse` task. They look at each env's workspace as it is, without fetching, so a
  * conversation never moves a reconciler's checkout.
  */
final case class ConverseTools(envs: List[EnvRuntime], manifests: ManifestSource):
  import ConverseTools.*

  def status(in: NoInput): IO[String, Statuses] =
    ZIO.foreach(envs): runtime =>
      (for
        head     <- runtime.workspace.git("rev-parse", "HEAD").flatMap(h => ZIO.fromEither(CommitSha.from(h)).mapError(AgentError.Git(_)))
        (cfg, _) <- RepoConfig.load(runtime.workspace.dir)
        apps     <- manifests.manifests(runtime.workspace.dir, cfg, head)
        deployed <- runtime.engine.deployed(apps.map(_.app), runtime.env).mapError(AgentError.Engine(_))
      yield EnvStatus(runtime.env.value, Some(head.value), deployed.map(_.value), apps.map(_.app.value), None))
        .catchAll(e => ZIO.succeed(EnvStatus(runtime.env.value, None, None, Nil, Some(e.message))))
    .map(Statuses(_))

  def plan(in: PlanInput): IO[String, PlanOutput] =
    for
      runtime  <- ZIO.fromOption(envs.find(_.env.value == in.env)).orElseFail(s"no env ${in.env}; envs are ${envs.map(_.env.value).mkString(", ")}")
      head     <- runtime.workspace.git("rev-parse", "HEAD").flatMap(h => ZIO.fromEither(CommitSha.from(h)).mapError(AgentError.Git(_))).mapError(_.message)
      (cfg, _) <- RepoConfig.load(runtime.workspace.dir).mapError(_.message)
      apps     <- manifests.manifests(runtime.workspace.dir, cfg, head).mapError(_.message)
      plans    <- ZIO.foreach(apps.flatMap(a => List(Phase.Expand, Phase.Contract).map(a -> _))): (m, phase) =>
                    runtime.engine.plan(m, runtime.env, Some(head), phase).mapBoth(_.message, PlanText.render)
    yield PlanOutput(plans.mkString("\n\n"))

  def policy(in: NoInput): IO[String, PolicyOutput] =
    ZIO.fromOption(envs.headOption).orElseFail("no envs").flatMap(r => RepoConfig.load(r.workspace.dir).mapBoth(_.message, (_, text) => PolicyOutput(text)))

object ConverseTools:
  final case class NoInput() derives Schema
  final case class EnvStatus(env: String, latest: Option[String], deployed: Option[String], apps: List[String], error: Option[String]) derives Schema
  final case class Statuses(envs: List[EnvStatus]) derives Schema
  final case class PlanInput(env: String) derives Schema
  final case class PlanOutput(plan: String) derives Schema
  final case class PolicyOutput(policy: String) derives Schema

/** The `Converse` task: answers people in the threads of the agent's messages and when they mention it, with
  * read-only tools. Only members of the policy's groups get answers, and only to the latest message, so a thread the
  * agent already answered gets nothing more.
  */
final case class Converse(slack: SlackApi, self: SlackIdentity, bedrock: Bedrock, instructions: String, tools: ConverseTools, envs: List[EnvRuntime]):
  import ConverseTools.*

  def handle(event: InboxEvent): UIO[Unit] =
    val thread = event match
      case InboxEvent.SlackReply(channel, ts, _)   => Some(channel -> ts)
      case InboxEvent.SlackMention(channel, ts, _) => Some(channel -> ts)
      case _                                       => None
    ZIO.foreachDiscard(thread)((channel, ts) => respond(channel, ts).catchAll(e => ZIO.logWarning(s"answering in $channel/$ts: $e")))

  private def ours(m: SlackMessage): Boolean = m.botId.contains(self.botId) || m.user.contains(self.userId)

  private def respond(channel: String, thread: String): IO[String, Unit] =
    for
      config     <- ZIO.fromOption(envs.headOption).orElseFail("no envs").flatMap(r => RepoConfig.load(r.workspace.dir).mapBoth(_.message, _._1))
      messages   <- slack.replies(channel, thread).mapError(_.message)
      responders <- ZIO.foreach((config.slack.owners :: config.slack.groups).distinct)(slack.groupMembers).mapBoth(_.message, _.flatten.toSet)
      involved    = messages.headOption.exists(ours) || messages.exists(_.text.contains(s"<@${self.userId}>"))
      eligible    = messages.lastOption.exists(m => !ours(m) && m.botId.isEmpty && m.user.exists(responders.contains))
      _ <- ZIO.when(channel == config.slack.channel && involved && eligible):
             answer(messages).flatMap(text => slack.post(channel, text, Some(thread), None).mapError(_.message))
    yield ()

  private def answer(messages: List[SlackMessage]): IO[String, String] =
    val transcript = messages.map(m => s"[${if ours(m) then "iae" else s"<@${m.user.getOrElse("?")}>"}] ${m.text}").mkString("\n")
    val prompt =
      s"""<thread>
         |$transcript
         |</thread>
         |
         |Answer the last message.""".stripMargin
    Bedrock
      .loop(
        prompt,
        (
          status = ToolHandler(tools.status, "Each env's latest commit on the branch, the commit deployed there, and its apps."),
          plan = ToolHandler(tools.plan, "What the agent would change in an env (by name) now, both phases."),
          policy = ToolHandler(tools.policy, "The team's approval policy, .iae/POLICY.md."),
        ),
      )
      .system(instructions)
      .inferenceConfig(InferenceConfig(maxTokens = 1024, temperature = 0.2))
      .maxIterations(6)
      .text
      .provideEnvironment(ZEnvironment(bedrock))
      .mapError(_.getMessage)
      .map(_.trim)
      .filterOrFail(_.nonEmpty)("the model returned no answer")

object Converse:
  def make(slack: SlackApi, self: SlackIdentity, bedrock: Bedrock, envs: List[EnvRuntime], manifests: ManifestSource): IO[AgentError, Converse] =
    Model.prompt("converse").mapBoth(AgentError.Config(_), Converse(slack, self, bedrock, _, ConverseTools(envs, manifests), envs))

/** Conversations, one at a time, so two events about the same thread can't both get an answer. */
final case class Conversations(converse: Converse, queue: Queue[InboxEvent]):
  def offer(event: InboxEvent): UIO[Unit] = queue.offer(event).unit
  def run: UIO[Nothing] = queue.take.flatMap(converse.handle).forever

object Conversations:
  def make(converse: Converse): UIO[Conversations] = Queue.sliding[InboxEvent](100).map(Conversations(converse, _))
