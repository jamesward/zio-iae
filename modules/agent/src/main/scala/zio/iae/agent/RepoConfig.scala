package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*
import zio.iae.engine.policy.{PolicyConfig, SlackGroup}
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** An app in the repo and the command (run in the repo) that prints its manifest, e.g. `bin/photo-app --iae-manifest`. */
final case class AppEntry(name: AppName, manifest: List[String]) derives Schema

final case class SlackSettings(channel: String, owners: SlackGroup, groups: List[SlackGroup]) derives Schema

/** `.iae/config.json` of a managed repo. `identities` maps commit author emails to Slack user ids, so an author can't
  * approve their own change.
  */
final case class RepoConfig(
    apps: List[AppEntry],
    envs: List[EnvName],
    production: List[EnvName],
    slack: SlackSettings,
    identities: Map[String, String] = Map.empty,
) derives Schema:
  def policy(text: String): PolicyConfig =
    PolicyConfig(text, slack.owners, (slack.owners :: slack.groups).toSet, production.toSet)

object RepoConfig:
  val path       = ".iae/config.json"
  val policyPath = ".iae/POLICY.md"

  def load(dir: Path): IO[AgentError, (RepoConfig, String)] =
    for
      json   <- read(dir.resolve(path).nn).someOrFail(AgentError.Config(s"$path is missing"))
      config <- ZIO.fromEither(JsonCodec.jsonDecoder(Schema[RepoConfig]).decodeJson(json)).mapError(e => AgentError.Config(s"$path: $e"))
      _      <- ZIO.fail(AgentError.Config(s"$path lists no envs")).when(config.envs.isEmpty)
      policy <- read(dir.resolve(policyPath).nn).map(_.getOrElse(""))
    yield (config, policy)

  private def read(file: Path): IO[AgentError, Option[String]] =
    ZIO.attemptBlocking(Option.when(Files.exists(file))(Files.readString(file, UTF_8).nn)).mapError(e => AgentError.Config(s"reading $file: ${e.getMessage}"))
