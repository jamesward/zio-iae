package zio.iae

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum

@simpleEnum
enum Billing derives Schema, CanEqual:
  case OnDemand

final case class TableSpec(billing: Billing = Billing.OnDemand) derives Schema, CanEqual

enum TableError derives CanEqual:
  /** A stored item doesn't decode as the table's value type, e.g. after an incompatible schema change. */
  case Undecodable(detail: String)
  case Unavailable(detail: String)

  def message: String = this match
    case Undecodable(detail) => s"stored item doesn't match the table's types: $detail"
    case Unavailable(detail) => s"table unavailable: $detail"

/** Reads a key-value table of `K` to `V`. The types come from the table's declaration. */
trait TableReader[I]:
  def get[K, V](key: K)(using I <:< Table.Id[K, V]): IO[TableError, Option[V]]

trait TableWriter[I]:
  def put[K, V](key: K, value: V)(using I <:< Table.Id[K, V]): IO[TableError, Unit]
  def delete[K, V](key: K)(using I <:< Table.Id[K, V]): IO[TableError, Unit]

trait Table[I] extends TableReader[I], TableWriter[I]

object Table:
  /** Declares a table of `K` to `V`, both stored as JSON from their schemas:
    * {{{
    * object Photos extends Table.Id[PhotoId, PhotoInfo](LogicalId("photos"))
    * }}}
    */
  abstract class Id[K, V](val logicalId: LogicalId, val spec: TableSpec = TableSpec())(using
      val keySchema: Schema[K],
      val valueSchema: Schema[V],
  )

  def reader[I <: Id[?, ?] & Singleton](id: I)(using Tag[TableReader[I]]): Infra[TableReader[I]] =
    declare(id, Chunk(Capability.TableRead))

  def readWrite[I <: Id[?, ?] & Singleton](id: I)(using Tag[Table[I]]): Infra[Table[I]] =
    declare(id, Chunk(Capability.TableRead, Capability.TableWrite))

  private def declare[I <: Id[?, ?] & Singleton, R >: Table[I]: Tag](id: I, capabilities: Chunk[Capability]): Infra[R] =
    Infra(
      resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Table(id.spec))),
      grants = capabilities.map(Grant(id.logicalId, ResourceKind.Table, _)),
      layer = ZLayer.fromZIO[Cloud, InfraError, R](ZIO.serviceWithZIO[Cloud](_.table[I](id))),
    )

  def encode[A](schema: Schema[A], value: A): String = zio.schema.codec.JsonCodec.jsonEncoder(schema).encodeJson(value, None).toString
  def decode[A](schema: Schema[A], json: String): Either[String, A] = zio.schema.codec.JsonCodec.jsonDecoder(schema).decodeJson(json)
