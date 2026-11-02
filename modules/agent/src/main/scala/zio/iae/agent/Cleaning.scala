package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.Plan

/** What a cleanup of one app in one env is judged and planned by: the commit deployed in the env (the newest that the
  * branch head's apps, and this one, were applied at), the config and policy at that commit, and whether the app is
  * abandoned (the branch head's config no longer declares it). The agent and the executor work it out the same way.
  */
final case class CleanupBasis(commit: CommitSha, config: RepoConfig, text: String, abandoned: Boolean):
  def cutoff(today: java.time.LocalDate): java.time.LocalDate = today.minusDays(config.orphanRetentionDays.toLong).nn

object Cleaning:
  /** How far back usage is read: for cleanup requests' context and the idle report. */
  val usageWindow: java.time.Duration = java.time.Duration.ofDays(30).nn

  /** None when nothing of the app is deployed in the env, or when the app isn't declared at the branch head and its
    * resources don't come from this repo. An app is abandoned when the head's config doesn't declare it: the push
    * that drops it applies nothing to it, so the deployed config may still name it.
    */
  def basis(runtime: EnvRuntime, app: AppName): IO[AgentError, Option[CleanupBasis]] =
    for
      _         <- runtime.workspace.sync
      (head, _) <- RepoConfig.load(runtime.workspace.dir)
      declared   = head.apps.exists(_.name == app)
      mine      <- if declared then ZIO.succeed(true) else ours(runtime, app)
      deployed  <- if mine then runtime.engine.deployed((head.apps.map(_.name) :+ app).distinct, runtime.env).mapError(AgentError.Engine(_)) else ZIO.none
      basis <- ZIO.foreach(deployed): commit =>
                 RepoConfig.loadAt(runtime.workspace, commit).map((config, text) => CleanupBasis(commit, config, text, !declared))
    yield basis

  /** Whether the app's resources in the env were applied from this repo: one of the commits they record is on the
    * branch. Another repo's apps can share an account; they aren't this agent's to clean up.
    */
  def ours(runtime: EnvRuntime, app: AppName): IO[AgentError, Boolean] =
    for
      head    <- runtime.workspace.git("rev-parse", "HEAD").flatMap(h => ZIO.fromEither(CommitSha.from(h)).mapError(AgentError.Git(_)))
      commits <- runtime.engine.commits(app, runtime.env).mapError(AgentError.Engine(_))
      found   <- ZIO.exists(commits)(c => runtime.workspace.contains(c, head).catchAll(_ => ZIO.succeed(false)))
    yield found

  /** For an approval request: what the cleanup does and why, resource by resource, with how much each is used. */
  def section(plan: Plan, basis: CleanupBasis, usage: Map[Address, ResourceUsage]): String =
    val retention = basis.config.orphanRetentionDays
    def name(a: Address) = s"${a.kind} ${a.id.value}"
    def used(a: Address) = usage.get(a).fold("")(u => s" (${u.summary})")
    val deletes = plan.changes.collect { case PlannedChange(Change.Delete(a), _, _, _) => a }
    val prunes  = plan.changes.collect { case PlannedChange(Change.Prune(a, versions), _, _, _) => (a, versions.size) }
    val orphans = plan.changes.collect { case PlannedChange(Change.Orphan(a, _), _, _, _) => a }
    val lines =
      Option.when(deletes.nonEmpty)(
        deletes.map(a => name(a) + used(a)).mkString(", ") + s" ${if deletes.size == 1 then "has" else "have"} been orphaned (removed from the manifest) " +
          s"for over $retention days; this deletes ${if deletes.size == 1 then "it" else "them"}."
      ).toList ++
        prunes.map((a, n) => s"${name(a)}: $n old task definition revision(s) that nothing runs or rolls back to (the newest ${basis.config.keepVersions} are kept).") ++
        Option.when(orphans.nonEmpty)(
          s"`${plan.app.value}` isn't declared in `.iae/config.json` any more, so this marks its resources orphaned: " +
            orphans.map(a => name(a) + used(a)).mkString(", ") + s". They're deleted after $retention days, through the policy again."
        ).toList
    s"*Cleanup:* ${lines.mkString(" ")}"
