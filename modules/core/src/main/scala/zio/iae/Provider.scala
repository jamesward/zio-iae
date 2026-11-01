package zio.iae

import zio.IO

/** Observes and changes one kind of resource. `diff` and `risk` are pure; `applyChange` must be idempotent so that a
  * re-run after a crash converges.
  */
trait Provider[S, O]:
  def kind: ResourceKind

  def observe(address: Address): IO[ProviderError, Option[O]]

  def diff(address: Address, desired: S, observed: Option[O]): Option[Change[S]]

  def risk(change: Change[S]): Risk

  def applyChange(change: Change[S]): IO[ProviderError, Unit]
