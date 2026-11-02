package zio.iae.testkit

import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** Compute Optimizer's `GetECSServiceRecommendations` with recommendations the test sets, as Floci doesn't emulate
  * Compute Optimizer. Point an `AwsConfig`'s endpoint at `url`.
  */
final case class ComputeOptimizerFake(url: URL, recommendations: Ref[Map[String, ComputeOptimizerFake.Advice]]):
  /** Recommends a service (by ARN suffix, e.g. `service/cluster/name`) other CPU units and memory MiB. */
  def recommend(service: String, finding: String, current: (Int, Int), recommended: (Int, Int)): UIO[Unit] =
    recommendations.update(_ + (service -> ComputeOptimizerFake.Advice(finding, current, recommended)))

object ComputeOptimizerFake:
  final case class Advice(finding: String, current: (Int, Int), recommended: (Int, Int))

  val make: ZIO[Scope, Throwable, ComputeOptimizerFake] =
    for
      recommendations <- Ref.make(Map.empty[String, Advice])
      server          <- Server.defaultWithPort(0).build
      port            <- Server.install(routes(recommendations)).provideEnvironment(server)
      url             <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield ComputeOptimizerFake(url, recommendations)

  private def routes(recommendations: Ref[Map[String, Advice]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / trailing -> handler: (req: Request) =>
        for
          body <- req.body.asString.orElseSucceed("")
          arns  = body.fromJson[Json.Obj].toOption.flatMap(_.get("serviceArns")).flatMap(_.asArray).getOrElse(Chunk.empty).flatMap(_.asString)
          all  <- recommendations.get
        yield Response.json(
          Json.Obj("ecsServiceRecommendations" -> Json.Arr(arns.flatMap: arn =>
            all.collectFirst { case (suffix, advice) if arn.endsWith(suffix) => advice }.map: a =>
              Json.Obj(
                "serviceArn"                  -> Json.Str(arn),
                "finding"                     -> Json.Str(a.finding),
                "currentServiceConfiguration" -> Json.Obj("cpu" -> Json.Num(a.current._1), "memory" -> Json.Num(a.current._2)),
                "serviceRecommendationOptions" -> Json.Arr(Json.Obj("cpu" -> Json.Num(a.recommended._1), "memory" -> Json.Num(a.recommended._2))),
              )
          *)).toJson
        ),
    )
