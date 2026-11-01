package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*
import zio.iae.engine.policy.{PolicyConfig, SlackGroup}
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** An app in the repo: the shell commands that build it, then the command that prints its manifest (both run at the
  * repo's root, e.g. `sbt photoApp/stage` and `target/.../bin/photo-app --iae-manifest`), and the build image for
  * CodeBuild if not the project's.
  */
final case class AppEntry(name: AppName, manifest: List[String], build: List[String] = Nil, image: Option[String] = None) derives Schema

final case class SlackSettings(channel: String, owners: SlackGroup, groups: List[SlackGroup]) derives Schema

/** An env's AWS account: the role ARNs the agent assumes there to plan (read-only) and to apply, its region if not
  * the agent's, and where its audit evidence goes.
  */
final case class EnvAccount(planner: String, deployer: String, region: Option[String] = None, audit: Option[AuditSettings] = None) derives Schema

/** `.iae/config.json` of a managed repo. `identities` maps commit author emails to Slack user ids, so an author can't
  * approve their own change. `accounts` maps env names to their accounts; an env without one uses the agent's own
  * account.
  */
final case class RepoConfig(
    apps: List[AppEntry],
    envs: List[EnvName],
    production: List[EnvName],
    slack: SlackSettings,
    identities: Map[String, String] = Map.empty,
    accounts: Map[String, EnvAccount] = Map.empty,
) derives Schema:
  def policy(text: String): PolicyConfig =
    PolicyConfig(text, slack.owners, (slack.owners :: slack.groups).toSet, production.toSet)

object RepoConfig:
  val path       = ".iae/config.json"
  val policyPath = ".iae/POLICY.md"

  private val encoder = JsonCodec.jsonEncoder(Schema[RepoConfig])

  def encode(config: RepoConfig): String = encoder.encodeJson(config, None).toString

  def load(dir: Path): IO[AgentError, (RepoConfig, String)] =
    read(dir.resolve(path).nn).zip(read(dir.resolve(policyPath).nn)).flatMap(parse)

  /** The config and policy as they were at an earlier commit. */
  def loadAt(workspace: GitWorkspace, commit: CommitSha): IO[AgentError, (RepoConfig, String)] =
    workspace.show(commit, path).zip(workspace.show(commit, policyPath)).flatMap(parse)

  private def parse(json: Option[String], policy: Option[String]): IO[AgentError, (RepoConfig, String)] =
    for
      json   <- ZIO.fromOption(json).orElseFail(AgentError.Config(s"$path is missing"))
      config <- ZIO.fromEither(JsonCodec.jsonDecoder(Schema[RepoConfig]).decodeJson(json)).mapError(e => AgentError.Config(s"$path: $e"))
      _      <- ZIO.fail(AgentError.Config(s"$path lists no envs")).when(config.envs.isEmpty)
    yield (config, policy.getOrElse(""))

  private def read(file: Path): IO[AgentError, Option[String]] =
    ZIO.attemptBlocking(Option.when(Files.exists(file))(Files.readString(file, UTF_8).nn)).mapError(e => AgentError.Config(s"reading $file: ${e.getMessage}"))
