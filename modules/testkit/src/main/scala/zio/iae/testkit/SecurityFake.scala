package zio.iae.testkit

import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** Security Hub (`GetFindings`), GuardDuty (`ListDetectors`, `ListFindings`, `GetFindings`), Inspector
  * (`ListFindings`) and IAM Access Analyzer (`ListAnalyzers`, `ListFindings`) at one endpoint, with findings the test
  * adds, as Floci emulates none of them. Point an `AwsConfig`'s endpoint at `url`.
  */
final case class SecurityFake(url: URL, findings: Ref[Map[String, List[Json]]]):
  private def add(service: String, finding: Json): UIO[Unit] = findings.update(m => m.updated(service, m.getOrElse(service, Nil) :+ finding))

  def hub(id: String, product: String, severity: String, title: String, resource: String): UIO[Unit] =
    add("hub", Json.Obj(
      "Id" -> Json.Str(id), "ProductName" -> Json.Str(product), "Severity" -> Json.Obj("Label" -> Json.Str(severity)),
      "Title" -> Json.Str(title), "Description" -> Json.Str(s"$title."), "Resources" -> Json.Arr(Json.Obj("Id" -> Json.Str(resource), "Type" -> Json.Str("AwsS3Bucket"))),
    ))

  /** A GuardDuty finding whose resource names `mentioning` (e.g. a bucket or a role). */
  def guardDuty(id: String, severity: Double, title: String, mentioning: String): UIO[Unit] =
    add("guardduty", Json.Obj(
      "id" -> Json.Str(id), "severity" -> Json.Num(severity), "title" -> Json.Str(title), "type" -> Json.Str("UnauthorizedAccess:IAMUser/InstanceCredentialExfiltration"),
      "description" -> Json.Str(s"$title."),
      "resource" -> Json.Obj("resourceType" -> Json.Str("AccessKey"), "accessKeyDetails" -> Json.Obj("userName" -> Json.Str(mentioning))),
    ))

  def image(repository: String, tag: String, vulnerability: String, severity: String, pkg: String, fixed: String): UIO[Unit] =
    add("inspector", Json.Obj(
      "findingArn" -> Json.Str(s"arn:aws:inspector2:us-east-1:000000000000:finding/$vulnerability-$tag"), "severity" -> Json.Str(severity),
      "title" -> Json.Str(s"$vulnerability - $pkg"), "description" -> Json.Str(s"$vulnerability in $pkg."),
      "packageVulnerabilityDetails" -> Json.Obj("vulnerabilityId" -> Json.Str(vulnerability),
        "vulnerablePackages" -> Json.Arr(Json.Obj("name" -> Json.Str(pkg), "version" -> Json.Str("1.0"), "fixedInVersion" -> Json.Str(fixed)))),
      "resources" -> Json.Arr(Json.Obj("type" -> Json.Str("AWS_ECR_CONTAINER_IMAGE"),
        "details" -> Json.Obj("awsEcrContainerImage" -> Json.Obj("repositoryName" -> Json.Str(repository), "imageTags" -> Json.Arr(Json.Str(tag)))))),
    ))

  def analyzer(id: String, resource: String, public: Boolean): UIO[Unit] =
    add("analyzer", Json.Obj(
      "id" -> Json.Str(id), "resource" -> Json.Str(resource), "isPublic" -> Json.Bool(public), "status" -> Json.Str("ACTIVE"),
      "principal" -> Json.Obj("AWS" -> Json.Str("arn:aws:iam::999999999999:root")), "action" -> Json.Arr(Json.Str("s3:GetObject")),
    ))

object SecurityFake:
  val make: ZIO[Scope, Throwable, SecurityFake] =
    for
      findings <- Ref.make(Map.empty[String, List[Json]])
      server   <- Server.defaultWithPort(0).build
      port     <- Server.install(routes(findings)).provideEnvironment(server)
      url      <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield SecurityFake(url, findings)

  private def strings(j: Option[Json]): List[String] =
    j.flatMap(_.asArray).fold(Nil)(_.toList.flatMap(_.asObject).flatMap(_.get("value")).flatMap(_.asString))

  private def inspected(f: Json, criteria: Option[Json.Obj]): Boolean =
    val repos = strings(criteria.flatMap(_.get("ecrImageRepositoryName")))
    val tags  = strings(criteria.flatMap(_.get("ecrImageTags")))
    val image = f.asObject.flatMap(_.get("resources")).flatMap(_.asArray).flatMap(_.headOption).flatMap(_.asObject)
      .flatMap(_.get("details")).flatMap(_.asObject).flatMap(_.get("awsEcrContainerImage")).flatMap(_.asObject)
    val repo     = image.flatMap(_.get("repositoryName")).flatMap(_.asString)
    val imageTag = image.flatMap(_.get("imageTags")).flatMap(_.asArray).fold(Nil)(_.toList.flatMap(_.asString))
    repo.exists(repos.contains) && imageTag.exists(tags.contains)

  private def routes(findings: Ref[Map[String, List[Json]]]): Routes[Any, Nothing] =
    Routes(
      Method.ANY / trailing -> handler: (req: Request) =>
        for
          body <- req.body.asString.orElseSucceed("")
          json  = body.fromJson[Json.Obj].getOrElse(Json.Obj())
          all  <- findings.get
          of    = (service: String) => all.getOrElse(service, Nil)
          path  = req.url.path.encode
          reply = (req.method.name, path) match
                    case ("POST", "/findings")      => Json.Obj("Findings" -> Json.Arr(of("hub")*))
                    case ("POST", "/findings/list") =>
                      val criteria = json.get("filterCriteria").flatMap(_.asObject)
                      Json.Obj("findings" -> Json.Arr(of("inspector").filter(inspected(_, criteria))*))
                    case ("GET", "/detector") => Json.Obj("detectorIds" -> Json.Arr(Json.Str("d1")))
                    case ("POST", "/detector/d1/findings") =>
                      Json.Obj("findingIds" -> Json.Arr(of("guardduty").flatMap(_.asObject).flatMap(_.get("id"))*))
                    case ("POST", "/detector/d1/findings/get") => Json.Obj("findings" -> Json.Arr(of("guardduty")*))
                    case ("GET", "/analyzer") =>
                      Json.Obj("analyzers" -> Json.Arr(Json.Obj("arn" -> Json.Str("arn:aws:access-analyzer:us-east-1:000000000000:analyzer/account"))))
                    case ("POST", "/finding") => Json.Obj("findings" -> Json.Arr(of("analyzer")*))
                    case _                         => Json.Obj("message" -> Json.Str(s"no route ${req.method} $path"))
        yield Response.json(reply.toJson),
    )
