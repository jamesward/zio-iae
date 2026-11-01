package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, ToolHandler}
import zio.*
import zio.iae.*
import zio.schema.{Schema, derived}

/** Explains why a reconcile failed. */
trait FailureDiagnoser:
  def diagnose(workspace: GitWorkspace, commit: CommitSha, env: EnvName, error: String): IO[String, String]

/** Read-only views of the repository at one commit, read from git's objects, so they never touch a checkout. */
final case class RepoAt(workspace: GitWorkspace, commit: CommitSha):
  import RepoAt.*

  def listFiles(in: ListInput): IO[String, Files] =
    workspace.git("ls-tree", "-r", "--name-only", commit.value, "--", in.directory.stripPrefix("/").stripSuffix("/") match { case "" => "."; case d => d })
      .mapBoth(_.message, out => Files(out.linesIterator.filter(_.nonEmpty).take(500).toList))

  def readFile(in: ReadInput): IO[String, FileContent] =
    workspace.git("show", s"${commit.value}:${in.path.stripPrefix("/")}").mapBoth(_.message, c => FileContent(in.path, c.take(20000)))

  def search(in: SearchInput): IO[String, Matches] =
    workspace.git("grep", "-n", "-E", "-e", in.pattern, commit.value)
      .map(out => Matches(out.linesIterator.map(_.stripPrefix(s"${commit.value}:")).take(200).toList))
      .catchAll(e => if e.message.contains("exited 1") then ZIO.succeed(Matches(Nil)) else ZIO.fail(e.message))

object RepoAt:
  final case class ListInput(directory: String) derives Schema
  final case class Files(paths: List[String]) derives Schema
  final case class ReadInput(path: String) derives Schema
  final case class FileContent(path: String, content: String) derives Schema
  final case class SearchInput(pattern: String) derives Schema
  final case class Matches(lines: List[String]) derives Schema

/** The `DiagnoseFailure` task: the model reads the failing commit with read-only tools and explains the failure. */
final case class DiagnoseFailure(bedrock: Bedrock, instructions: String) extends FailureDiagnoser:
  def diagnose(workspace: GitWorkspace, commit: CommitSha, env: EnvName, error: String): IO[String, String] =
    val repo = RepoAt(workspace, commit)
    val prompt =
      s"""Env `${env.value}`, commit `${commit.value}`.
         |
         |<error>
         |${error.take(8000)}
         |</error>""".stripMargin
    Bedrock
      .loop(
        prompt,
        (
          listFiles = ToolHandler(repo.listFiles, "The paths of the files under a directory of the repository ('' for all)."),
          readFile = ToolHandler(repo.readFile, "A file's content at the failing commit, by path from the repository's root."),
          search = ToolHandler(repo.search, "Lines matching an extended regular expression, as path:line:text."),
        ),
      )
      .system(instructions)
      .inferenceConfig(InferenceConfig(maxTokens = 1500, temperature = 0.2))
      .maxIterations(12)
      .text
      .provideEnvironment(ZEnvironment(bedrock))
      .mapError(_.getMessage)
      .map(_.trim)
      .filterOrFail(_.nonEmpty)("the model returned no diagnosis")

object DiagnoseFailure:
  def make(bedrock: Bedrock): IO[AgentError, FailureDiagnoser] =
    Model.prompt("diagnose-failure").mapBoth(AgentError.Config(_), DiagnoseFailure(bedrock, _))
