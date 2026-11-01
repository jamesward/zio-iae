package zio.iae.cli

import java.nio.file.{Path, Paths}
import zio.iae.*
import zio.iae.engine.Phase

enum ManifestSource derives CanEqual:
  case Stdin
  case File(path: Path)

final case class Target(env: EnvName, manifest: ManifestSource, commit: Option[CommitSha], repository: Path)

enum Command derives CanEqual:
  case Plan(target: Target, phases: List[Phase], json: Boolean)
  case Apply(target: Target, breakGlass: Option[String])
  case Help

object Command:
  val usage: String =
    """usage: iae <command> [options]
      |
      |commands:
      |  plan   show what would change (both phases unless --phase is given)
      |  apply  plan and apply, if the default policy lets it go ahead
      |
      |options:
      |  --env <name>            the env to plan for (required)
      |  --manifest <file|->     the app's manifest (default: stdin, e.g. `photo-app --iae-manifest | iae plan ...`)
      |  --commit <sha>          the commit being deployed; enables the commit guard
      |  --repo <dir>            git clone used to order commits (default: .)
      |  --phase expand|contract plan only one phase
      |  --json                  print plans as JSON
      |  --break-glass <reason>  apply even though policy needs approval (recorded)
      |
      |environment: AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_ENDPOINT_URL,
      |  IAE_NAME_SUFFIX""".stripMargin

  def parse(args: List[String]): Either[String, Command] =
    args match
      case Nil | ("help" | "--help" | "-h") :: _ => Right(Help)
      case "plan" :: rest =>
        for
          opts   <- options(rest, flags = Set("--json"))
          target <- target(opts)
          phases <- opts.get("--phase") match
                      case None             => Right(List(Phase.Expand, Phase.Contract))
                      case Some("expand")   => Right(List(Phase.Expand))
                      case Some("contract") => Right(List(Phase.Contract))
                      case Some(other)      => Left(s"--phase must be expand or contract, not '$other'")
        yield Plan(target, phases, opts.contains("--json"))
      case "apply" :: rest =>
        for
          opts   <- options(rest, flags = Set.empty)
          target <- target(opts)
          reason <- opts.get("--break-glass") match
                      case Some(r) if r.isBlank => Left("--break-glass needs a reason")
                      case other                => Right(other)
        yield Apply(target, reason)
      case other :: _ => Left(s"unknown command '$other'")

  private val withValue = Set("--env", "--manifest", "--commit", "--repo", "--phase", "--break-glass")

  private def options(args: List[String], flags: Set[String]): Either[String, Map[String, String]] =
    args match
      case Nil                                        => Right(Map.empty)
      case flag :: rest if flags.contains(flag)       => options(rest, flags).map(_ + (flag -> ""))
      case name :: value :: rest if withValue(name)   => options(rest, flags).map(_ + (name -> value))
      case name :: Nil if withValue(name)             => Left(s"$name needs a value")
      case other :: _                                 => Left(s"unknown option '$other'")

  private def target(opts: Map[String, String]): Either[String, Target] =
    for
      envName <- opts.get("--env").toRight("--env is required")
      env     <- EnvName.from(envName).left.map(_.message)
      commit  <- opts.get("--commit").fold(Right(None))(c => CommitSha.from(c).map(Some(_)))
      manifest = opts.get("--manifest") match
                   case None | Some("-") => ManifestSource.Stdin
                   case Some(path)       => ManifestSource.File(Paths.get(path).nn)
    yield Target(env, manifest, commit, Paths.get(opts.getOrElse("--repo", ".")).nn)
