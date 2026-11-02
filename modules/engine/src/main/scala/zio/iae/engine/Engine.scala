package zio.iae.engine

import zio.*
import zio.iae.*

final case class Drivers(all: List[ResourceDriver]):
  def apply(kind: ResourceKind): IO[EngineError, ResourceDriver] =
    ZIO.fromOption(all.find(_.kind == kind)).orElseFail(EngineError.NoDriver(kind))

final case class Planner(drivers: Drivers, discovery: Discovery):
  def plan(manifest: Manifest, env: EnvName, target: Option[CommitSha], phase: Phase): IO[EngineError, Plan] =
    val changes = phase match
      case Phase.Expand   => expand(manifest, env)
      case Phase.Contract => contract(manifest, env)
    changes.map(Plan(manifest.app, env, target, ContentHash.of(manifest), phase, _))

  /** What cleanup proposes for an app in an env, as one plan:
    *   - deleting its resources orphaned on `before` or earlier (the kinds that are deleted, not services), each
    *     assessed by its provider;
    *   - pruning old versions of its live services' deployments beyond the newest `keep`;
    *   - for an app no repo declares any more (`abandoned`), orphaning whatever of it isn't yet, which starts its
    *     retention.
    *
    * The plan's hash depends only on its changes, so a proposal stays the same from one day to the next until
    * something else qualifies.
    */
  def cleanup(app: AppName, env: EnvName, before: java.time.LocalDate, keep: Int = 5, abandoned: Boolean = false): IO[EngineError, Plan] =
    for
      found   <- discovery.managed(app, env).mapError(EngineError.Provider(_))
      today   <- Clock.localDateTime.map(_.toLocalDate.nn)
      ordered  = found.sortBy(d => (d.address.kind.ordinal, d.address.id))
      expired  = ordered.filter(_.orphanedSince.exists(!_.isAfter(before)))
      deletes <- ZIO.foreach(expired)(d => drivers(d.address.kind).flatMap(_.deletion(d.address).mapError(EngineError.Provider(_))))
      live     = ordered.filter(_.orphanedSince.isEmpty)
      prunes  <- ZIO.foreach(if abandoned then Nil else live)(d => drivers(d.address.kind).flatMap(_.prune(d.address, keep).mapError(EngineError.Provider(_))))
      orphans  = if abandoned then live.map(d => PlannedChange(Change.Orphan(d.address, today), Risk.Low, Set.empty)) else Nil
    yield Plan(app, env, None, ContentHash.of("cleanup"), Phase.Contract, deletes.flatten ++ prunes.flatten ++ orphans)

  /** Back to the version a resource ran before: a plan with that one change, or none if there's nothing to go back
    * to. It isn't for a commit: the resources keep the commit they record.
    */
  def rollback(address: Address): IO[EngineError, Plan] =
    drivers(address.kind)
      .flatMap(_.rollback(address).mapError(EngineError.Provider(_)))
      .map(change => Plan(address.app, address.env, None, ContentHash.of(s"rollback ${address.show}"), Phase.Expand, change.toList))

  private def expand(manifest: Manifest, env: EnvName): IO[EngineError, List[PlannedChange]] =
    ZIO
      .foreach(manifest.resources.sortBy(_.kind.ordinal)): decl =>
        val address = Address(manifest.app, env, decl.kind, decl.id)
        drivers(decl.kind).flatMap(_.plan(address, decl.spec).mapError(EngineError.Provider(_)))
      .map(_.flatten)

  private def contract(manifest: Manifest, env: EnvName): IO[EngineError, List[PlannedChange]] =
    val declared = manifest.resources.map(d => Address(manifest.app, env, d.kind, d.id)).toSet
    for
      found <- discovery.managed(manifest.app, env).mapError(EngineError.Provider(_))
      today <- Clock.localDateTime.map(_.toLocalDate.nn)
    yield found
      .filter(d => !declared.contains(d.address) && d.orphanedSince.isEmpty)
      .sortBy(_.address.show)
      .map(d => PlannedChange(Change.Orphan(d.address, today), Risk.Low, Set.empty))

/** What an apply did: the changes applied, in order. */
final case class ApplyReport(plan: ContentHash, applied: List[Address])

final case class Executor(drivers: Drivers, commits: CommitOrder):
  /** Applies a plan's changes in order. Every change is idempotent, so after a failure the next plan is simply what is
    * left to do.
    */
  def apply(plan: Plan): IO[EngineError, ApplyReport] =
    ZIO
      .foreach(plan.changes): planned =>
        for
          driver <- drivers(planned.address.kind)
          _      <- guard(driver, planned.address, plan.target)
          _      <- driver.apply(planned.change, ApplyContext(plan.target)).mapError(EngineError.Provider(_))
          _      <- ZIO.logInfo(s"applied ${planned.change.productPrefix} ${planned.address.show}")
        yield planned.address
      .map(ApplyReport(plan.hash, _))

  private def guard(driver: ResourceDriver, address: Address, target: Option[CommitSha]): IO[EngineError, Unit] =
    ZIO.foreachDiscard(target): t =>
      driver.lastCommit(address).mapError(EngineError.Provider(_)).flatMap:
        case Some(last) if last != t =>
          commits.isAncestor(t, last).flatMap(newer => ZIO.fail(EngineError.Guarded(address, last, t)).when(newer))
        case _ => ZIO.unit

final case class Engine(
    planner: Planner,
    executor: Executor,
    watch: Watch = Watch.none,
    attribution: Attribution = Attribution.none,
    pricing: Pricing = Pricing.none,
    access: Access = Access.none,
    recommendations: Recommendations = Recommendations.none,
    activity: Activity = Activity.none,
):
  /** The apps with managed resources in the env. */
  def apps(env: EnvName): IO[EngineError, Set[AppName]] = planner.discovery.apps(env).mapError(EngineError.Provider(_))

  /** The commits the app's resources in the env were last changed by. */
  def commits(app: AppName, env: EnvName): IO[EngineError, Set[CommitSha]] =
    planner.discovery.managed(app, env).mapBoth(EngineError.Provider(_), _.flatMap(_.commit).toSet)

  /** The artifacts (images) the app's resources in the env run or would roll back to. */
  def artifacts(app: AppName, env: EnvName): IO[EngineError, Set[String]] =
    planner.discovery.managed(app, env).mapError(EngineError.Provider(_)).flatMap: found =>
      ZIO.foreach(found)(d => planner.drivers(d.address.kind).flatMap(_.artifacts(d.address).mapError(EngineError.Provider(_)))).map(_.flatten.toSet)

  /** How much each of the app's resources in the env (orphans included) has been used since a time. */
  def usage(app: AppName, env: EnvName, since: java.time.Instant): IO[EngineError, List[ResourceUsage]] =
    planner.discovery.managed(app, env).mapError(EngineError.Provider(_)).flatMap: found =>
      ZIO.foreach(found.sortBy(d => (d.address.kind.ordinal, d.address.id)))(d => activity.usage(d.address, since).mapError(EngineError.Provider(_))).map(_.flatten)

  /** Right-sizing recommendations for the app's live services. */
  def rightSizing(app: AppName, env: EnvName): IO[EngineError, List[RightSizing]] =
    recommendations.services(app, env).mapError(EngineError.Provider(_))

  /** The app's runtime role's permissions not used since a time. */
  def unusedPermissions(app: AppName, env: EnvName, since: java.time.Instant): IO[EngineError, List[UnusedPermission]] =
    access.unused(app, env, since).mapError(EngineError.Provider(_))

  /** Who changed a resource since a time, by its write calls other than the pipeline's. */
  def changedBy(address: Address, since: java.time.Instant): IO[EngineError, List[ChangedBy]] =
    attribution.changes(address, since).mapError(EngineError.Provider(_))

  /** The alarms watching the app's live services in the env. */
  def alarms(app: AppName, env: EnvName): IO[EngineError, List[WatchedAlarm]] = watch.alarms(app, env).mapError(EngineError.Provider(_))

  def plan(manifest: Manifest, env: EnvName, target: Option[CommitSha], phase: Phase): IO[EngineError, Plan] =
    planner.plan(manifest, env, target, phase).flatMap(costed)

  /** Each change with its projected monthly cost. An estimate that fails is left out, never fails the plan. */
  private def costed(plan: Plan): UIO[Plan] =
    ZIO
      .foreach(plan.changes): c =>
        pricing.estimate(c)
          .catchAll(e => ZIO.logWarning(s"estimating the cost of ${c.address.show}: ${e.message}").as(None))
          .map(cost => c.copy(cost = cost))
      .map(changes => plan.copy(changes = changes))

  def apply(plan: Plan): IO[EngineError, ApplyReport] = executor.apply(plan)

  /** What cleanup proposes for the app in the env (see `Planner.cleanup`). */
  def cleanup(app: AppName, env: EnvName, before: java.time.LocalDate, keep: Int = 5, abandoned: Boolean = false): IO[EngineError, Plan] =
    planner.cleanup(app, env, before, keep, abandoned).flatMap(costed)

  /** The plan that puts a resource back to the version it ran before; empty if there's none. */
  def rollback(address: Address): IO[EngineError, Plan] = planner.rollback(address)

  /** The newest commit any of the apps' managed resources in the env were changed by (their `iae:commit` tags), or
    * None before the first apply. Commits on diverged branches can't be ordered and fail.
    */
  def deployed(apps: List[AppName], env: EnvName): IO[EngineError, Option[CommitSha]] =
    for
      found <- ZIO.foreach(apps)(app => planner.discovery.managed(app, env)).mapError(EngineError.Provider(_))
      commits = found.flatten.flatMap(_.commit).distinct
      newest <- ZIO.filter(commits)(c => ZIO.forall(commits.filterNot(_ == c))(other => executor.commits.isAncestor(other, c)))
      _      <- ZIO.fail(EngineError.CommitOrder(s"no newest of ${commits.map(_.short).mkString(", ")}")).when(commits.nonEmpty && newest.isEmpty)
    yield newest.headOption

  /** Plans and applies both phases without any approval step. For dev envs, tests and the CLI's `apply`; the agent
    * puts policy and approvals between planning and applying.
    */
  def converge(manifest: Manifest, env: EnvName, target: Option[CommitSha]): IO[EngineError, List[ApplyReport]] =
    ZIO.foreach(List(Phase.Expand, Phase.Contract))(phase => plan(manifest, env, target, phase).flatMap(apply))

object Engine:
  def make(drivers: Drivers, discovery: Discovery, commits: CommitOrder, watch: Watch = Watch.none): Engine =
    Engine(Planner(drivers, discovery), Executor(drivers, commits), watch)
