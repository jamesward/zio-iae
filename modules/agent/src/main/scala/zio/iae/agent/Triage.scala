package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, ToolHandler}
import zio.*
import zio.iae.*
import zio.schema.{Schema, derived}

/** The `Triage` task: the model investigates an alert with read-only tools (the env's alarms, what's deployed and the
  * recent commits, and the repository at the live commit) and writes its findings for the alert's thread. With `prs`
  * it can also propose a fix as a pull request, like `DiagnoseFailure`.
  */
final case class Triage(bedrock: Bedrock, instructions: String, prs: Option[PullRequests] = None) extends AlertTriage:
  import Triage.*

  def triage(alert: Alert, runtime: EnvRuntime): IO[String, String] =
    val workspace = runtime.workspace
    val commit    = alert.alarm.commit
    val prompt =
      s"""Alert in `${alert.where}`: ${alert.headline}.""".stripMargin
    val alarms = ToolHandler(
      (_: NoInput) =>
        runtime.engine.alarms(alert.app, alert.env).mapBoth(_.message, as => Alarms(as.map(a => AlarmInfo(a.address.id.value, a.gate, a.firing, a.image)))),
      "Every alarm watching the app's live services in this env, and whether it is firing.",
    )
    val deployment = ToolHandler(
      (_: NoInput) =>
        workspace.git("log", "--format=%h %cI %an: %s", "-n", "15", s"origin/${workspace.branch}")
          .mapBoth(_.message, log => Deployment(alert.alarm.image, commit.map(_.value), log.linesIterator.toList)),
      "What is live (image and commit) and the branch's last 15 commits (short sha, time, author: subject).",
    )
    val config = InferenceConfig(maxTokens = 1500, temperature = 0.2)
    val answer = ZIO.fromOption(commit).orElse(workspace.sync.mapError(_.message)).flatMap: at =>
      val repo      = RepoAt(workspace, at)
      val listFiles = ToolHandler(repo.listFiles, "The paths of the files under a directory of the repository ('' for all), at the live commit.")
      val readFile  = ToolHandler(repo.readFile, "A file's content at the live commit, by path from the repository's root.")
      val search    = ToolHandler(repo.search, "Lines matching an extended regular expression at the live commit, as path:line:text.")
      prs match
        case None =>
          Bedrock.loop(prompt, (alarms = alarms, deployment = deployment, listFiles = listFiles, readFile = readFile, search = search))
            .system(instructions).inferenceConfig(config).maxIterations(14).text
            .provideEnvironment(ZEnvironment(bedrock)).mapError(_.getMessage)
        case Some(p) =>
          ZIO.scoped:
            FixWorkspace.make(workspace, at, p).flatMap: fix =>
              Bedrock
                .loop(
                  prompt,
                  (
                    alarms = alarms,
                    deployment = deployment,
                    listFiles = listFiles,
                    readFile = readFile,
                    search = search,
                    writeFile = ToolHandler(fix.writeFile, "Replace a file's whole content in the fix (a scratch copy at the live commit)."),
                    openPullRequest = ToolHandler(fix.openPullRequest, "Commit the files written so far and open one pull request with them."),
                  ),
                )
                .system(s"$instructions\n\n${DiagnoseFailure.fixes}")
                .inferenceConfig(config)
                .maxIterations(18)
                .text
                .provideEnvironment(ZEnvironment(bedrock))
                .mapError(_.getMessage)
    answer.map(_.trim).filterOrFail(_.nonEmpty)("the model returned no findings")

object Triage:
  final case class NoInput() derives Schema
  final case class AlarmInfo(service: String, gate: String, firing: Boolean, image: Option[String]) derives Schema
  final case class Alarms(alarms: List[AlarmInfo]) derives Schema
  final case class Deployment(image: Option[String], commit: Option[String], recentCommits: List[String]) derives Schema

  def make(bedrock: Bedrock, prs: Option[PullRequests] = None): IO[AgentError, AlertTriage] =
    Model.prompt("triage").mapBoth(AgentError.Config(_), Triage(bedrock, _, prs))
