package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, ToolHandler}
import zio.*
import zio.iae.*

/** Reviews a background job's findings (unused permissions, cost recommendations) against the code, and says what to
  * do about them.
  */
trait FindingsReviewer:
  def review(workspace: GitWorkspace, commit: CommitSha, findings: String): IO[String, String]

/** The `ReviewSecurity` and `ReviewCost` tasks: the model reads the deployed commit with read-only tools and explains
  * the findings; with `prs` it can also change the code (narrow a capability, right-size a service) and open one pull
  * request, which then goes through the pipeline and policy like any change.
  */
final case class ReviewFindings(bedrock: Bedrock, instructions: String, prs: Option[PullRequests] = None) extends FindingsReviewer:
  def review(workspace: GitWorkspace, commit: CommitSha, findings: String): IO[String, String] =
    val repo   = RepoAt(workspace, commit)
    val prompt = s"Commit `${commit.value}`.\n\n<findings>\n${findings.take(8000)}\n</findings>"
    val listFiles = ToolHandler(repo.listFiles, "The paths of the files under a directory of the repository ('' for all).")
    val readFile  = ToolHandler(repo.readFile, "A file's content at the deployed commit, by path from the repository's root.")
    val search    = ToolHandler(repo.search, "Lines matching an extended regular expression, as path:line:text.")
    val config    = InferenceConfig(maxTokens = 1500, temperature = 0.2)
    val answer = prs match
      case None =>
        Bedrock.loop(prompt, (listFiles = listFiles, readFile = readFile, search = search))
          .system(instructions).inferenceConfig(config).maxIterations(14).text
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
                  writeFile = ToolHandler(fix.writeFile, "Replace a file's whole content in the change (a scratch copy at the deployed commit)."),
                  openPullRequest = ToolHandler(fix.openPullRequest, "Commit the files written so far and open one pull request with them."),
                ),
              )
              .system(s"$instructions\n\n${DiagnoseFailure.fixes}")
              .inferenceConfig(config)
              .maxIterations(18)
              .text
              .provideEnvironment(ZEnvironment(bedrock))
              .mapError(_.getMessage)
    answer.map(_.trim).filterOrFail(_.nonEmpty)("the model returned no review")

object ReviewFindings:
  /** `task` is the prompt's name: `review-security` or `review-cost`. */
  def make(bedrock: Bedrock, task: String, prs: Option[PullRequests] = None): IO[AgentError, FindingsReviewer] =
    Model.prompt(task).mapBoth(AgentError.Config(_), ReviewFindings(bedrock, _, prs))
