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

  /** Removing old versions of the resource's deployment beyond the newest `keep`, never one that runs or is the
    * rollback target: None if there's nothing to remove or the kind has no versions.
    */
  def prune(address: Address, keep: Int): IO[ProviderError, Option[PlannedChange]] = zio.ZIO.none

  /** The artifacts (container images) the resource runs or would roll back to, which must be kept. */
  def artifacts(address: Address): IO[ProviderError, List[String]] = zio.ZIO.succeed(Nil)

/** A managed resource found in the cloud by its tags. */
final case class Discovered(address: Address, commit: Option[CommitSha], orphanedSince: Option[LocalDate]) derives CanEqual

trait Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]]

  /** The apps with managed resources in the env, whichever repo declares them (or none any more). */
  def apps(env: EnvName): IO[ProviderError, Set[AppName]] = zio.ZIO.succeed(Set.empty)

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

  /** What an existing resource costs a month now, by the same model: None when the kind has no cost model. */
  def monthly(address: Address): IO[ProviderError, Option[Double]] = zio.ZIO.none

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

/** A cheaper way to store a resource's data: what was found, the change to its declaration that makes it, and what that
  * saves a month (USD, an estimate).
  */
final case class StorageFinding(address: Address, finding: String, change: String, monthlySavings: Double) derives CanEqual

/** What the cloud recommends for an app's resources: right-sizing services (Compute Optimizer), and storing data for
  * less.
  */
trait Recommendations:
  def services(app: AppName, env: EnvName): IO[ProviderError, List[RightSizing]]

  def storage(app: AppName, env: EnvName): IO[ProviderError, List[StorageFinding]] = zio.ZIO.succeed(Nil)

object Recommendations:
  val none: Recommendations = (_, _) => zio.ZIO.succeed(Nil)

/** How much a resource is used over a window, in words for people ("12.4 GB in 3,100 objects"), and whether it's
  * idle: no requests, messages or reads at all.
  */
final case class ResourceUsage(address: Address, idle: Boolean, summary: String) derives CanEqual

/** How much the managed resources are used, from the cloud's metrics. None for a kind it can't tell. */
trait Activity:
  def usage(address: Address, since: java.time.Instant): IO[ProviderError, Option[ResourceUsage]]

object Activity:
  val none: Activity = (_, _) => zio.ZIO.none

/** What an app actually cost in an env over a period, from the bill (USD): in total and by AWS service. */
final case class Spend(from: LocalDate, to: LocalDate, total: Double, byService: Map[String, Double]) derives CanEqual

/** A spike in an env's spending that the cloud's anomaly detection found: how much more than expected (USD), since
  * when, on what, and the causes it names.
  */
final case class CostAnomaly(id: String, start: Option[LocalDate], dimension: Option[String], impact: Double, causes: List[String]) derives CanEqual

/** The bill: what the managed resources actually cost (Cost Explorer by the `iae:app` and `iae:env` tags), and the
  * spending anomalies in an env's account.
  */
trait Bills:
  def spend(app: AppName, env: EnvName, from: LocalDate, to: LocalDate): IO[ProviderError, Option[Spend]]
  def anomalies(env: EnvName, since: LocalDate): IO[ProviderError, List[CostAnomaly]]

object Bills:
  val none: Bills = new Bills:
    def spend(app: AppName, env: EnvName, from: LocalDate, to: LocalDate) = zio.ZIO.none
    def anomalies(env: EnvName, since: LocalDate)                         = zio.ZIO.succeed(Nil)
