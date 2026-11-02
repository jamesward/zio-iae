package zio.iae.aws.client

import zio.*
import zio.http.Method
import zio.json.*
import zio.json.ast.Json
import AwsRestJsonClient.*

/** A finding from one of AWS's security services, in the shape they share: a stable id, a severity label
  * (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`), a title and description, the resources it names (ARNs or names), and the
  * whole finding's JSON as text, for matching names it mentions anywhere.
  */
final case class RawFinding(id: String, severity: String, title: String, description: String, resources: List[String], raw: String)

/** Security Hub's active findings (`GetFindings`) on resources with the given tags. Security Hub collects other
  * services' findings too (GuardDuty, Inspector, Config); `product` says whose.
  */
final case class SecurityHubClient(rest: AwsRestJsonClient):
  private val service = AwsService("securityhub", "securityhub")

  def findings(tags: Map[String, String]): IO[AwsError, List[(String, RawFinding)]] =
    val filters = Json.Obj(
      "ResourceTags"   -> Json.Arr(tags.toList.sorted.map((k, v) => Json.Obj("Key" -> Json.Str(k), "Value" -> Json.Str(v), "Comparison" -> Json.Str("EQUALS")))*),
      "RecordState"    -> Json.Arr(Json.Obj("Value" -> Json.Str("ACTIVE"), "Comparison" -> Json.Str("EQUALS"))),
      "WorkflowStatus" -> Json.Arr(Json.Obj("Value" -> Json.Str("NEW"), "Comparison" -> Json.Str("EQUALS")), Json.Obj("Value" -> Json.Str("NOTIFIED"), "Comparison" -> Json.Str("EQUALS"))),
    )
    def page(token: Option[String], left: Int): IO[AwsError, List[Json]] =
      val body = Json.Obj(List("Filters" -> filters, "MaxResults" -> Json.Num(100)) ++ token.map("NextToken" -> Json.Str(_)).toList*)
      rest.call(service, Method.POST, "/findings", Some(body)).flatMap: r =>
        val found = r.items("Findings")
        r.str("NextToken").filter(t => t.nonEmpty && left > 1).fold(ZIO.succeed(found))(t => page(Some(t), left - 1).map(found ++ _))
    page(None, 10).map(_.map: f =>
      f.str("ProductName").getOrElse("Security Hub") -> RawFinding(
        f.str("Id").getOrElse(""),
        f.path("Severity", "Label").flatMap(_.asString).getOrElse("INFORMATIONAL"),
        f.str("Title").getOrElse(""),
        f.str("Description").getOrElse(""),
        f.items("Resources").flatMap(_.str("Id")),
        f.toJson,
      ))

/** GuardDuty's current findings (not archived) of at least medium severity, from the account's detector. */
final case class GuardDutyClient(rest: AwsRestJsonClient):
  private val service = AwsService("guardduty", "guardduty")

  def findings: IO[AwsError, List[RawFinding]] =
    rest.call(service, Method.GET, "/detector").map(_.items("detectorIds").flatMap(_.asString)).flatMap: detectors =>
      ZIO.foreach(detectors.headOption.toList): detector =>
        val criteria = Json.Obj("criterion" -> Json.Obj(
          "service.archived" -> Json.Obj("eq" -> Json.Arr(Json.Str("false"))),
          "severity"         -> Json.Obj("gte" -> Json.Num(4)),
        ))
        for
          listed <- rest.call(service, Method.POST, s"/detector/$detector/findings", Some(Json.Obj("findingCriteria" -> criteria, "maxResults" -> Json.Num(50))))
          ids     = listed.items("findingIds").flatMap(_.asString)
          found  <- if ids.isEmpty then ZIO.succeed(Json.Obj())
                    else rest.call(service, Method.POST, s"/detector/$detector/findings/get", Some(Json.Obj("findingIds" -> Json.Arr(ids.map(Json.Str(_))*))))
        yield found.items("findings").map: f =>
          val severity = f.num("severity").getOrElse(0d)
          RawFinding(
            f.str("id").getOrElse(""),
            if severity >= 7 then "HIGH" else if severity >= 4 then "MEDIUM" else "LOW",
            f.str("title").getOrElse(f.str("type").getOrElse("")),
            f.str("description").getOrElse(""),
            f.field("resource").flatMap(_.str("resourceType")).toList,
            f.toJson,
          )
    .map(_.flatten)

/** Inspector's active vulnerability findings for container images in an ECR repository, with the given tags. */
final case class InspectorClient(rest: AwsRestJsonClient):
  private val service = AwsService("inspector2", "inspector2")

  def imageFindings(repository: String, tags: List[String]): IO[AwsError, List[RawFinding]] =
    if tags.isEmpty then ZIO.succeed(Nil)
    else
      val criteria = Json.Obj(
        "ecrImageRepositoryName" -> Json.Arr(Json.Obj("comparison" -> Json.Str("EQUALS"), "value" -> Json.Str(repository))),
        "ecrImageTags"           -> Json.Arr(tags.map(t => Json.Obj("comparison" -> Json.Str("EQUALS"), "value" -> Json.Str(t)))*),
        "findingStatus"          -> Json.Arr(Json.Obj("comparison" -> Json.Str("EQUALS"), "value" -> Json.Str("ACTIVE"))),
      )
      def page(token: Option[String], left: Int): IO[AwsError, List[Json]] =
        val body = Json.Obj(List("filterCriteria" -> criteria, "maxResults" -> Json.Num(100)) ++ token.map("nextToken" -> Json.Str(_)).toList*)
        rest.call(service, Method.POST, "/findings/list", Some(body)).flatMap: r =>
          val found = r.items("findings")
          r.str("nextToken").filter(t => t.nonEmpty && left > 1).fold(ZIO.succeed(found))(t => page(Some(t), left - 1).map(found ++ _))
      page(None, 10).map(_.map: f =>
        val vulnerability = f.path("packageVulnerabilityDetails", "vulnerabilityId").flatMap(_.asString)
        val packages = f.path("packageVulnerabilityDetails").toList.flatMap(_.items("vulnerablePackages")).map: p =>
          List(p.str("name"), p.str("version").map("@" + _), p.str("fixedInVersion").map(v => s" (fixed in $v)")).flatten.mkString
        RawFinding(
          f.str("findingArn").getOrElse(""),
          f.str("severity").getOrElse("UNTRIAGED"),
          vulnerability.fold(f.str("title").getOrElse(""))(v => s"$v in ${packages.mkString(", ")}"),
          f.str("description").getOrElse(""),
          f.items("resources").flatMap(r => r.path("details", "awsEcrContainerImage").toList.flatMap(_.items("imageTags")).flatMap(_.asString)),
          f.toJson,
        ))

/** IAM Access Analyzer's active findings from the account's analyzer: resources shared outside the account, or
  * publicly.
  */
final case class AccessAnalyzerClient(rest: AwsRestJsonClient):
  private val service = AwsService("access-analyzer", "access-analyzer")

  def findings: IO[AwsError, List[RawFinding]] =
    rest.call(service, Method.GET, "/analyzer", query = List("type" -> "ACCOUNT")).map(_.items("analyzers").flatMap(_.str("arn"))).flatMap: analyzers =>
      ZIO.foreach(analyzers.headOption.toList): analyzer =>
        val body = Json.Obj("analyzerArn" -> Json.Str(analyzer), "filter" -> Json.Obj("status" -> Json.Obj("eq" -> Json.Arr(Json.Str("ACTIVE")))), "maxResults" -> Json.Num(100))
        rest.call(service, Method.POST, "/finding", Some(body)).map(_.items("findings").map: f =>
          val public = f.bool("isPublic").contains(true)
          val who    = f.field("principal").flatMap(_.asObject).fold("")(_.fields.map((k, v) => s"$k ${v.asString.getOrElse(v.toJson)}").mkString(", "))
          RawFinding(
            f.str("id").getOrElse(""),
            if public then "HIGH" else "MEDIUM",
            if public then "shared publicly" else s"shared outside the account${if who.nonEmpty then s" ($who)" else ""}",
            f.items("action").flatMap(_.asString).mkString(", "),
            f.str("resource").toList,
            f.toJson,
          ))
    .map(_.flatten)
