package zio.iae

import zio.*
import zio.schema.{Schema, derived}

/** A secret's value is set out of band (by a person or another system); zio-iae creates it, tags it and grants
  * read access, but never sees or stores the value.
  */
final case class SecretSpec(description: String = "") derives Schema, CanEqual

final case class SecretValue(value: String):
  override def toString: String = "SecretValue(***)"

enum SecretError derives CanEqual:
  /** The secret exists but nobody has set its value yet. */
  case NotSet(id: LogicalId)
  case Unavailable(detail: String)

  def message: String = this match
    case NotSet(id)          => s"secret '${id.value}' has no value yet"
    case Unavailable(detail) => s"secret unavailable: $detail"

trait SecretReader[I]:
  def read: IO[SecretError, SecretValue]

object Secret:
  /** {{{ object DatabasePassword extends Secret.Id(LogicalId("database-password")) }}} */
  abstract class Id(val logicalId: LogicalId, val spec: SecretSpec = SecretSpec())

  def reader[I <: Id & Singleton](id: I)(using Tag[SecretReader[I]]): Infra[SecretReader[I]] =
    Infra(
      resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Secret(id.spec))),
      grants = Chunk(Grant(id.logicalId, ResourceKind.Secret, Capability.SecretRead)),
      layer = ZLayer.fromZIO[Cloud, InfraError, SecretReader[I]](ZIO.serviceWithZIO[Cloud](_.secret[I](id.logicalId, id.spec))),
    )
