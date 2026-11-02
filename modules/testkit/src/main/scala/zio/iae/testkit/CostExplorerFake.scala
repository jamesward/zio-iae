package zio.iae.testkit

import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** Cost Explorer's `GetCostAndUsage` and `GetAnomalies`, with costs and anomalies the test sets, as Floci doesn't
  * emulate Cost Explorer. Costs are by the tags they're for (all must match the request's filter) and AWS service.
  * Point an `AwsConfig`'s endpoint at `url`; `requests` has every request body, to check what was asked.
  */
final case class CostExplorerFake(
    url: URL,
    costs: Ref[List[(Map[String, String], String, Double)]],
    anomalies: Ref[List[CostExplorerFake.Anomaly]],
    requests: Ref[List[Json.Obj]],
):
  def cost(tags: Map[String, String], service: String, amount: Double): UIO[Unit] = costs.update(_ :+ (tags, service, amount))

  def anomaly(a: CostExplorerFake.Anomaly): UIO[Unit] = anomalies.update(_ :+ a)

object CostExplorerFake:
  final case class Anomaly(id: String, start: String, dimension: String, impact: Double, service: String)

  val make: ZIO[Scope, Throwable, CostExplorerFake] =
    for
      costs     <- Ref.make(List.empty[(Map[String, String], String, Double)])
      anomalies <- Ref.make(List.empty[Anomaly])
      requests  <- Ref.make(List.empty[Json.Obj])
      server    <- Server.defaultWithPort(0).build
      port      <- Server.install(routes(costs, anomalies, requests)).provideEnvironment(server)
      url       <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield CostExplorerFake(url, costs, anomalies, requests)

  /** The tags a request's filter asks for: one `Tags` expression, or an `And` of them. */
  private def tagsOf(filter: Option[Json]): Map[String, String] =
    def leaf(j: Json): Option[(String, String)] =
      for
        tags  <- j.asObject.flatMap(_.get("Tags")).flatMap(_.asObject)
        key   <- tags.get("Key").flatMap(_.asString)
        value <- tags.get("Values").flatMap(_.asArray).flatMap(_.headOption).flatMap(_.asString)
      yield key -> value
    filter.flatMap(_.asObject).fold(Map.empty): f =>
      f.get("And").flatMap(_.asArray).fold(leaf(f).toMap)(_.flatMap(leaf).toMap)

  private def costReply(all: List[(Map[String, String], String, Double)], wanted: Map[String, String]): Json =
    val groups = all
      .filter((tags, _, _) => wanted.forall((k, v) => tags.get(k).contains(v)))
      .groupMapReduce(_._2)(_._3)(_ + _)
      .toList
      .sorted
      .map: (service, amount) =>
        Json.Obj("Keys" -> Json.Arr(Json.Str(service)), "Metrics" -> Json.Obj("UnblendedCost" -> Json.Obj("Amount" -> Json.Str(amount.toString), "Unit" -> Json.Str("USD"))))
    Json.Obj("ResultsByTime" -> Json.Arr(Json.Obj("Groups" -> Json.Arr(groups*), "Total" -> Json.Obj())))

  private def anomalyReply(all: List[Anomaly]): Json =
    val found = all.map: a =>
      Json.Obj(
        "AnomalyId"        -> Json.Str(a.id),
        "AnomalyStartDate" -> Json.Str(a.start),
        "DimensionValue"   -> Json.Str(a.dimension),
        "RootCauses"       -> Json.Arr(Json.Obj("Service" -> Json.Str(a.service), "Region" -> Json.Str("us-east-1"))),
        "Impact"           -> Json.Obj("MaxImpact" -> Json.Num(a.impact / 2), "TotalImpact" -> Json.Num(a.impact)),
      )
    Json.Obj("Anomalies" -> Json.Arr(found*))

  private def routes(costs: Ref[List[(Map[String, String], String, Double)]], anomalies: Ref[List[Anomaly]], requests: Ref[List[Json.Obj]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / trailing -> handler: (req: Request) =>
        for
          body  <- req.body.asString.orElseSucceed("")
          json   = body.fromJson[Json.Obj].getOrElse(Json.Obj())
          _     <- requests.update(_ :+ json)
          target = req.rawHeader("x-amz-target").getOrElse("")
          reply <-
            if target.endsWith(".GetCostAndUsage") then costs.get.map(costReply(_, tagsOf(json.get("Filter"))))
            else if target.endsWith(".GetAnomalies") then anomalies.get.map(anomalyReply)
            else ZIO.succeed(Json.Obj("__type" -> Json.Str("UnknownOperationException")))
        yield Response.json(reply.toJson),
    )
