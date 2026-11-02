package zio.iae

import zio.*

/** A static, inspectable description of the resources a program uses, plus the layer that binds them at runtime. */
final case class Infra[+R](
    resources: Chunk[ResourceDecl],
    grants: Chunk[Grant],
    layer: ZLayer[Cloud, InfraError, R],
):
  def ++[R2: Tag](that: Infra[R2]): Infra[R & R2] =
    Infra(resources ++ that.resources, (grants ++ that.grants).distinct, layer ++ that.layer)

  def manifest(app: AppName): Either[InfraError, Manifest] =
    val byId = resources.groupBy(_.id).toList.sortBy(_._1)
    val conflict = byId.collectFirst:
      case (id, decls) if decls.map(_.spec).distinct.size > 1 =>
        InfraError.ConflictingDeclarations(id, decls.map(_.spec).distinct.toList)
    val queues = resources.collect { case ResourceDecl(id, ResourceSpec.Queue(_)) => id }.toSet
    val unknownSubscriber = resources.collectFirst:
      case ResourceDecl(topic, ResourceSpec.Topic(spec)) if spec.subscribers.exists(s => !queues.contains(s.queue)) =>
        val missing = spec.subscribers.map(_.queue).filterNot(queues.contains).map(_.value).mkString(", ")
        InfraError.InvalidConfig(s"topic '${topic.value}' delivers to queues the app doesn't declare: $missing (declare them, e.g. with MessageQueue.wired)")
    val invalidService = resources.collectFirst(Function.unlift:
      case ResourceDecl(id, ResourceSpec.Service(spec)) => Service.invalid(spec).map(why => InfraError.InvalidConfig(s"service '${id.value}': $why"))
      case _                                            => None
    )
    val sortedGrants = grants.toList.sortBy(g => (g.id, g.capability.ordinal))
    val role = Option.when(sortedGrants.nonEmpty):
      ResourceDecl(Manifest.runtimeRole, ResourceSpec.Role(RoleSpec(sortedGrants, List(TrustedService.EcsTasks))))
    conflict.orElse(unknownSubscriber).orElse(invalidService).toLeft(Manifest(app = app, resources = byId.flatMap(_._2.headOption) ++ role.toList, grants = sortedGrants))
