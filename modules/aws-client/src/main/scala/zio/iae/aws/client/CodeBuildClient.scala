package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}

/** The CodeBuild calls the agent makes: a project per build (its source, buildspec, image and artifacts set when it's
  * created), starting it, reading the build's status and deleting the project.
  */
final case class CodeBuildClient(json: AwsJsonClient):
  import CodeBuildClient.*

  def projectExists(name: String): IO[AwsError, Boolean] =
    json.call[Names, Projects](protocol, "BatchGetProjects", Names(List(name))).map(_.projects.exists(_.name == name))

  def createProject(project: CreateProject): IO[AwsError, Unit] =
    json.call[CreateProject, AwsJsonClient.Empty](protocol, "CreateProject", project).unit

  def deleteProject(name: String): IO[AwsError, Unit] =
    json.call[ProjectName, AwsJsonClient.Empty](protocol, "DeleteProject", ProjectName(name)).unit

  def startBuild(project: String): IO[AwsError, BuildId] =
    json.call[StartBuild, Started](protocol, "StartBuild", StartBuild(project)).map(s => BuildId(s.build.id))

  def build(id: BuildId): IO[AwsError, Option[Build]] =
    json.call[Ids, Builds](protocol, "BatchGetBuilds", Ids(List(id.value))).map(_.builds.find(_.id == id.value))

object CodeBuildClient:
  val layer: ZLayer[AwsJsonClient, Nothing, CodeBuildClient] = ZLayer.fromFunction(CodeBuildClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("codebuild", "codebuild"), "CodeBuild_20161006", "1.1")

  final case class BuildId(value: String)

  final case class Names(names: List[String]) derives Schema
  final case class ProjectName(name: String) derives Schema
  final case class Projects(projects: List[ProjectName]) derives Schema
  final case class Source(`type`: String, location: Option[String], buildspec: Option[String]) derives Schema
  final case class Artifacts(
      `type`: String,
      location: Option[String],
      path: Option[String],
      name: Option[String],
      packaging: Option[String],
      namespaceType: Option[String],
  ) derives Schema
  final case class EnvironmentVariable(name: String, value: String, `type`: String = "PLAINTEXT") derives Schema
  final case class Environment(`type`: String, image: String, computeType: String, environmentVariables: Option[List[EnvironmentVariable]])
      derives Schema
  final case class Tag(key: String, value: String) derives Schema
  final case class CreateProject(
      name: String,
      source: Source,
      artifacts: Artifacts,
      environment: Environment,
      serviceRole: String,
      timeoutInMinutes: Int,
      tags: List[Tag],
  ) derives Schema

  final case class StartBuild(projectName: String) derives Schema

  final case class BuildRef(id: String) derives Schema
  final case class Started(build: BuildRef) derives Schema
  final case class Ids(ids: List[String]) derives Schema
  final case class PhaseContext(statusCode: Option[String], message: Option[String]) derives Schema
  final case class Phase(phaseType: String, phaseStatus: Option[String], contexts: Option[List[PhaseContext]]) derives Schema
  final case class Build(id: String, buildStatus: String, buildComplete: Boolean, phases: Option[List[Phase]]) derives Schema:
    /** The failed phases and their messages, for a failure report. */
    def failures: List[String] =
      phases.getOrElse(Nil).filter(p => p.phaseStatus.exists(s => s != "SUCCEEDED")).map: p =>
        (p.phaseType :: p.contexts.getOrElse(Nil).flatMap(c => c.message.filter(_.nonEmpty))).mkString(": ")
  final case class Builds(builds: List[Build]) derives Schema
