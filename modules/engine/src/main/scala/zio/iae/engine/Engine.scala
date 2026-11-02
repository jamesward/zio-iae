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

final case class Engine(planner: Planner, executor: Executor, watch: Watch = Watch.none):
  /** The alarms watching the app's live services in the env. */
  def alarms(app: AppName, env: EnvName): IO[EngineError, List[ServiceAlarm]] = watch.alarms(app, env).mapError(EngineError.Provider(_))

  def plan(manifest: Manifest, env: EnvName, target: Option[CommitSha], phase: Phase): IO[EngineError, Plan] =
    planner.plan(manifest, env, target, phase)

  def apply(plan: Plan): IO[EngineError, ApplyReport] = executor.apply(plan)

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
