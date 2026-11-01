package zio.iae

import java.time.LocalDate
import zio.IO

/** Observes and changes one kind of resource. `diff` and `risk` are pure; `applyChange` must be idempotent so that a
  * re-run after a crash converges.
  */
trait Provider[S, O]:
  def kind: ResourceKind

  def observe(address: Address): IO[ProviderError, Option[O]]

  def diff(address: Address, desired: S, observed: Option[O]): Option[Change[S]]

  def risk(change: Change[S]): Risk

  def applyChange(change: Change[S], context: ApplyContext): IO[ProviderError, Unit]

/** What the engine needs from a resource kind, without knowing its spec or observed types. */
trait ResourceDriver:
  def kind: ResourceKind

  /** Observe the resource and work out what has to change to reach `desired`. */
  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]]

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit]

  /** The commit that last changed the resource, if it exists and records one. */
  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]]

/** A managed resource found in the cloud by its tags. */
final case class Discovered(address: Address, commit: Option[CommitSha], orphanedSince: Option[LocalDate]) derives CanEqual

trait Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]]
