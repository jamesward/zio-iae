package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Compute Optimizer's ECS recommendations for an app's live services that aren't optimized, with the monthly
  * savings priced from the same price book as plans (Fargate vCPU and memory × the live task count × hours).
  */
final case class ComputeOptimizerRecommendations(optimizer: ComputeOptimizerClient, provider: ServiceProvider, discovery: Discovery, book: PriceBook)
    extends Recommendations:
  def services(app: AppName, env: EnvName): IO[ProviderError, List[RightSizing]] =
    discovery.managed(app, env).flatMap: found =>
      ZIO.foreach(found.filter(_.address.kind == ResourceKind.Service))(d => live(d.address)).map(_.flatten).flatMap: services =>
        optimizer.ecsServices(services.map(_._2)).mapError(e => ProviderError(Address(app, env, ResourceKind.Service, LogicalId("web")), "recommendations", e.message)).map: found =>
          for
            (address, arn, count) <- services
            r                     <- found.find(_.serviceArn == arn).toList if r.finding != "Optimized"
            (cpu, memory)         <- r.recommended.headOption.toList
          yield RightSizing(address, r.finding, r.cpu, r.memory, cpu, memory, round(monthly(r.cpu, r.memory, count) - monthly(cpu, memory, count)))

  /** The live color's ECS service ARN and task count. */
  private def live(address: Address): IO[ProviderError, Option[(Address, String, Int)]] =
    provider.observe(address).flatMap:
      case None => ZIO.none
      case Some(current) =>
        val cluster = ServiceNaming.cluster(address)
        val name    = ServiceNaming.ecsService(address, current.active)
        provider.ecs.service(cluster, name).mapBoth(
          e => ProviderError(address, "observe", e.message),
          _.map(s => (address, s"arn:aws:ecs:${provider.context.region.value}:${provider.context.account.value}:service/$cluster/$name", s.desiredCount.max(1))),
        )

  private def monthly(cpu: Int, memory: Int, count: Int): Double =
    (cpu / 1024d * book("fargate.vcpu-hour") + memory / 1024d * book("fargate.gb-hour")) * count * PriceBook.hoursPerMonth

  private def round(d: Double): Double = math.round(d * 100) / 100d
