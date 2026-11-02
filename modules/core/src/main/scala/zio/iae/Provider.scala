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

  def assess(change: Change[S], observed: Option[O]): Assessment

  def applyChange(change: Change[S], context: ApplyContext): IO[ProviderError, Unit]

/** What the engine needs from a resource kind, without knowing its spec or observed types. */
trait ResourceDriver:
  def kind: ResourceKind

  /** Observe the resource and work out what has to change to reach `desired`. */
  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]]

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit]

  /** The commit that last changed the resource, if it exists and records one. */
  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]]

  /** Deleting the resource, as cleanup proposes it for an orphan: None if it doesn't exist or the kind isn't deleted. */
  def deletion(address: Address): IO[ProviderError, Option[PlannedChange]] = zio.ZIO.none

  /** Back to the version that was live before, if the kind can do that and has one. */
  def rollback(address: Address): IO[ProviderError, Option[PlannedChange]] = zio.ZIO.none

/** A managed resource found in the cloud by its tags. */
final case class Discovered(address: Address, commit: Option[CommitSha], orphanedSince: Option[LocalDate]) derives CanEqual

trait Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]]

/** An alarm watching a deployed resource: a live service's gates (those of the version with the traffic, which keep
  * watching it after its canary), or a resource's default alarms (a queue's age and dead letters, a table's
  * throttles). `gate` names it as an alert or rollback would ("5xx", "latency", "age", or the app's own alarm).
  */
final case class WatchedAlarm(address: Address, alarm: String, gate: String, firing: Boolean, image: Option[String], commit: Option[CommitSha])
    derives CanEqual

/** What watches the managed resources once they're deployed. */
trait Watch:
  def alarms(app: AppName, env: EnvName): IO[ProviderError, List[WatchedAlarm]]

object Watch:
  val none: Watch = (_, _) => zio.ZIO.succeed(Nil)

/** What a planned change will cost: its monthly cost before and after. None when the kind has no cost model. */
trait Pricing:
  def estimate(change: PlannedChange): IO[ProviderError, Option[CostDelta]]

object Pricing:
  val none: Pricing = _ => zio.ZIO.none

/** A change made to a resource, as the cloud's audit log recorded it: what, by whom, and when. */
final case class ChangedBy(action: String, by: String, at: java.time.Instant) derives CanEqual

/** Who changed a resource outside the pipeline: its write calls by anyone other than the pipeline's own deployer. */
trait Attribution:
  def changes(address: Address, since: java.time.Instant): IO[ProviderError, List[ChangedBy]]

object Attribution:
  val none: Attribution = (_, _) => zio.ZIO.succeed(Nil)

/** A permission an app's runtime role has but hasn't used: one statement of its generated policy, named for the grant
  * it comes from (e.g. `BucketWriteUploads`).
  */
final case class UnusedPermission(statement: String, actions: List[String], resources: List[String]) derives CanEqual

/** What an app's runtime identity actually uses of what it's granted. */
trait Access:
  /** The permissions not used since `since`; none for a role younger than that, which hasn't had the time. */
  def unused(app: AppName, env: EnvName, since: java.time.Instant): IO[ProviderError, List[UnusedPermission]]

object Access:
  val none: Access = (_, _, _) => zio.ZIO.succeed(Nil)

/** A right-sizing recommendation for a live service: from its size now (CPU units, memory MiB) to the recommended
  * one, and what that saves a month (USD; negative when it needs more).
  */
final case class RightSizing(address: Address, finding: String, cpu: Int, memory: Int, toCpu: Int, toMemory: Int, monthlySavings: Double)
    derives CanEqual

/** What the cloud recommends for an app's resources (Compute Optimizer). */
trait Recommendations:
  def services(app: AppName, env: EnvName): IO[ProviderError, List[RightSizing]]

object Recommendations:
  val none: Recommendations = (_, _) => zio.ZIO.succeed(Nil)
