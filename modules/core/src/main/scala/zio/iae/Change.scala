package zio.iae

import zio.NonEmptyChunk

/** One attribute that differs between the desired and observed state, for display in plans. */
final case class FieldChange(field: String, from: String, to: String) derives CanEqual

enum Change[+S] derives CanEqual:
  case Create(address: Address, spec: S)
  case Update(address: Address, spec: S, fields: NonEmptyChunk[FieldChange])
  case Delete(address: Address)

  def address: Address

enum Risk derives CanEqual:
  case Low, Medium, High, Critical

object Risk:
  given Ordering[Risk] = Ordering.by(_.ordinal)
