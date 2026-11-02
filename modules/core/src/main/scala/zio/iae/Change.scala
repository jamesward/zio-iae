package zio.iae

import java.time.LocalDate
import zio.NonEmptyChunk
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum

/** One attribute that differs between the desired and observed state, for display in plans. */
final case class FieldChange(field: String, from: String, to: String) derives Schema, CanEqual

enum Change[+S] derives CanEqual:
  case Create(address: Address, spec: S)
  case Update(address: Address, spec: S, fields: NonEmptyChunk[FieldChange])

  /** No longer declared: tagged as orphaned and left in place. Deleting is a separate, explicit decision. */
  case Orphan(address: Address, since: LocalDate)
  case Delete(address: Address)

  /** Back to the version that was live before (`to`), away from the live one (`from`), which is marked failed so it
    * isn't deployed again. Not planned from a manifest: asked for when an alert points at the live version.
    */
  case Rollback(address: Address, from: String, to: String)

  /** Old versions of a resource's deployment that nothing runs or rolls back to any more (a service's task
    * definition revisions beyond the newest few): removed. The resource itself is untouched.
    */
  case Prune(address: Address, versions: List[String])

  def address: Address

  def map[T](f: S => T): Change[T] = this match
    case Create(address, spec)         => Create(address, f(spec))
    case Update(address, spec, fields) => Update(address, f(spec), fields)
    case Orphan(address, since)        => Orphan(address, since)
    case Delete(address)               => Delete(address)
    case Rollback(address, from, to)   => Rollback(address, from, to)
    case Prune(address, versions)      => Prune(address, versions)

object Change:
  given [S: Schema]: Schema[Change[S]] = Schema.derived

@simpleEnum
enum Risk derives Schema, CanEqual:
  case Low, Medium, High, Critical

object Risk:
  given Ordering[Risk] = Ordering.by(_.ordinal)

/** Properties of a change that policies commonly care about, worked out by the provider that understands it. */
@simpleEnum
enum ChangeFlag derives Schema, CanEqual:
  case IamWidening, PublicAccess, EncryptionChange, DataLoss

  /** The resource was changed outside the pipeline since it was converged to the spec it still records. */
  case Drift

/** How risky a change is, and why. */
final case class Assessment(risk: Risk, flags: Set[ChangeFlag]) derives CanEqual

/** A change to one resource of any kind, as the engine plans, shows and applies it. */
/** A change's projected monthly cost in USD, before and after it, and how it was worked out: `exact` when the spec
  * fixes it (a service's tasks), otherwise estimated from usage, with the assumptions that went into it.
  */
final case class CostDelta(before: Double, after: Double, exact: Boolean, assumptions: List[String] = Nil) derives Schema, CanEqual:
  def delta: Double = after - before

final case class PlannedChange(change: Change[ResourceSpec], risk: Risk, flags: Set[ChangeFlag], cost: Option[CostDelta] = None)
    derives Schema, CanEqual:
  def address: Address = change.address
