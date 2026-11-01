package zio.iae.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*

enum CliError derives CanEqual:
  case Manifest(detail: String)
  case Failed(error: EngineError)

  def message: String = this match
    case Manifest(detail) => s"could not read the manifest: $detail"
    case Failed(error)    => error.message

/** Runs a command against an engine. Exit codes: 0 done, 1 failed, 2 policy needs approval or denies. */
object Run:
  val needsApproval: ExitCode = ExitCode(2)

  /** Without an LLM the CLI applies the default risk table: dev is free, prod is strict. */
  val owners: SlackGroup    = SlackGroup.from("owners").toOption.get
  val approvers: SlackGroup = SlackGroup.from("approvers").toOption.get
  val table                 = RiskTable(approvers, owners, production = Set(EnvName("prod")), free = Set(EnvName("dev")))
  val policy                = PolicyConfig("", owners, Set(owners, approvers), production = Set(EnvName("prod")))

  def execute(command: Command): ZIO[Engine, CliError, ExitCode] = command match
    case Command.Help => Console.printLine(Command.usage).orDie.as(ExitCode.success)
    case Command.Plan(target, phases, json) =>
      for
        manifest <- readManifest(target.manifest)
        _ <- ZIO.foreachDiscard(phases): phase =>
               for
                 plan     <- ZIO.serviceWithZIO[Engine](_.plan(manifest, target.env, target.commit, phase)).mapError(CliError.Failed(_))
                 decision <- decide(plan)
                 _        <- show(if json then plan.toJson else s"${PlanText.render(plan)}\n${PlanText.render(decision)}\n")
               yield ()
      yield ExitCode.success
    case Command.Apply(target, breakGlass) =>
      readManifest(target.manifest).flatMap: manifest =>
        ZIO.foldLeft(List(Phase.Expand, Phase.Contract))(ExitCode.success): (code, phase) =>
          if code.code != 0 then ZIO.succeed(code)
          else applyPhase(manifest, target, phase, breakGlass)

  private def applyPhase(manifest: Manifest, target: Target, phase: Phase, breakGlass: Option[String]): ZIO[Engine, CliError, ExitCode] =
    for
      engine   <- ZIO.service[Engine]
      plan     <- engine.plan(manifest, target.env, target.commit, phase).mapError(CliError.Failed(_))
      decision <- decide(plan)
      _        <- show(PlanText.render(plan))
      code <- (decision.decision, breakGlass) match
                case _ if plan.isEmpty => ZIO.succeed(ExitCode.success)
                case (Decision.Auto, _) => applied(engine, plan)
                case (_, Some(reason)) =>
                  ZIO.logWarning(s"break-glass apply of plan ${plan.hash.short} for ${target.env.value}: $reason") *>
                    show(s"${PlanText.render(decision)}\nbreak-glass: applying anyway ($reason)") *> applied(engine, plan)
                case _ => show(s"${PlanText.render(decision)}\nnot applied") .as(needsApproval)
    yield code

  private def applied(engine: Engine, plan: Plan): IO[CliError, ExitCode] =
    engine.apply(plan).mapError(CliError.Failed(_)).flatMap(r => show(s"applied ${r.applied.size} change(s)")).as(ExitCode.success)

  private def decide(plan: Plan): UIO[PlanDecision] =
    Clock.instant.map: now =>
      val facts = ChangeFacts.of(plan, Origin.Human, None, now)
      Verdicts.decide(facts, table.evaluate(facts), policy)

  private def show(text: String): UIO[Unit] = Console.printLine(text).orDie

  private def readManifest(source: ManifestSource): IO[CliError, Manifest] =
    val text = source match
      case ManifestSource.Stdin      => ZIO.attemptBlocking(String(java.lang.System.in.nn.readAllBytes().nn, UTF_8))
      case ManifestSource.File(path) => ZIO.attemptBlocking(Files.readString(path, UTF_8).nn)
    text
      .mapError(e => CliError.Manifest(e.getMessage))
      .flatMap(json => ZIO.fromEither(Manifest.fromJson(json)).mapError(CliError.Manifest(_)))
