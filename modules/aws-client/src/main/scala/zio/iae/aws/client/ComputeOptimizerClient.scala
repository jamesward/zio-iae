package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}

/** A Compute Optimizer recommendation for an ECS service: its finding (`Overprovisioned`, `Underprovisioned`,
  * `Optimized`) and the CPU (units) and memory (MiB) it recommends, best first.
  */
final case class EcsRecommendation(serviceArn: String, finding: String, cpu: Int, memory: Int, recommended: List[(Int, Int)])

/** Compute Optimizer's right-sizing recommendations, what the cost optimizer reads. */
final case class ComputeOptimizerClient(json: AwsJsonClient):
  import ComputeOptimizerClient.*

  def ecsServices(arns: List[String]): IO[AwsError, List[EcsRecommendation]] =
    if arns.isEmpty then ZIO.succeed(Nil)
    else
      json.call[Request, Response](protocol, "GetECSServiceRecommendations", Request(arns)).map: r =>
        r.ecsServiceRecommendations.getOrElse(Nil).map: e =>
          val current = e.currentServiceConfiguration
          EcsRecommendation(
            e.serviceArn,
            e.finding.getOrElse("Optimized"),
            current.flatMap(_.cpu).getOrElse(0),
            current.flatMap(_.memory).getOrElse(0),
            e.serviceRecommendationOptions.getOrElse(Nil).flatMap(o => o.cpu.zip(o.memory)),
          )

object ComputeOptimizerClient:
  private val protocol = AwsJsonProtocol(AwsService("compute-optimizer", "compute-optimizer"), "ComputeOptimizerService", "1.0")

  final case class Request(serviceArns: List[String]) derives Schema
  final case class Configuration(cpu: Option[Int], memory: Option[Int]) derives Schema
  final case class Option_(cpu: Option[Int], memory: Option[Int]) derives Schema
  final case class Recommendation(
      serviceArn: String,
      finding: Option[String],
      currentServiceConfiguration: Option[Configuration],
      serviceRecommendationOptions: Option[List[Option_]],
  ) derives Schema
  final case class Response(ecsServiceRecommendations: Option[List[Recommendation]]) derives Schema
