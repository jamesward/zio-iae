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

/** The `DiagnoseFailure` task: the model reads the failing commit with read-only tools and explains the failure.
  * With `prs`, it can also write files in a scratch clone and open one pull request with the fix.
  */
final case class DiagnoseFailure(bedrock: Bedrock, instructions: String, prs: Option[PullRequests] = None) extends FailureDiagnoser:
  def diagnose(workspace: GitWorkspace, commit: CommitSha, env: EnvName, error: String): IO[String, String] =
    val repo = RepoAt(workspace, commit)
    val prompt =
      s"""Env `${env.value}`, commit `${commit.value}`.
         |
         |<error>
         |${error.take(8000)}
         |</error>""".stripMargin
    val listFiles = ToolHandler(repo.listFiles, "The paths of the files under a directory of the repository ('' for all).")
    val readFile  = ToolHandler(repo.readFile, "A file's content at the failing commit, by path from the repository's root.")
    val search    = ToolHandler(repo.search, "Lines matching an extended regular expression, as path:line:text.")
    val config    = InferenceConfig(maxTokens = 1500, temperature = 0.2)
    val answer = prs match
      case None =>
        Bedrock.loop(prompt, (listFiles = listFiles, readFile = readFile, search = search))
          .system(instructions).inferenceConfig(config).maxIterations(12).text
          .provideEnvironment(ZEnvironment(bedrock)).mapError(_.getMessage)
      case Some(p) =>
        ZIO.scoped:
          FixWorkspace.make(workspace, commit, p).flatMap: fix =>
            Bedrock
              .loop(
                prompt,
                (
                  listFiles = listFiles,
                  readFile = readFile,
                  search = search,
                  writeFile = ToolHandler(fix.writeFile, "Replace a file's whole content in the fix (a scratch copy at the failing commit)."),
                  openPullRequest = ToolHandler(fix.openPullRequest, "Commit the files written so far and open one pull request with them."),
                ),
              )
              .system(s"$instructions\n\n${DiagnoseFailure.fixes}")
              .inferenceConfig(config)
              .maxIterations(16)
              .text
              .provideEnvironment(ZEnvironment(bedrock))
              .mapError(_.getMessage)
    answer.map(_.trim).filterOrFail(_.nonEmpty)("the model returned no diagnosis")

object DiagnoseFailure:
  /** Added to the instructions when the agent can open pull requests. */
  val fixes: String =
    """You can also propose the fix: `writeFile` replaces a file's content in a scratch copy of the failing commit, and
      |`openPullRequest` opens one pull request with what you wrote. Do it only when you are confident the fix is small
      |and right; people review it before it's merged. Then mention the pull request's link in your reply.""".stripMargin

  def make(bedrock: Bedrock, prs: Option[PullRequests] = None): IO[AgentError, FailureDiagnoser] =
    Model.prompt("diagnose-failure").mapBoth(AgentError.Config(_), DiagnoseFailure(bedrock, _, prs))
