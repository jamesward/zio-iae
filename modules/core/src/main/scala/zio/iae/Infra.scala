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
    conflict.toLeft(
      Manifest(
        app = app,
        resources = byId.flatMap(_._2.headOption),
        grants = grants.toList.sortBy(g => (g.id, g.capability.ordinal)),
      )
    )
