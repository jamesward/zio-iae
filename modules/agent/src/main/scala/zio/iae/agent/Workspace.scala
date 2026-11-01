package zio.iae.agent

import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*

/** A clone of the managed repo, one per env so envs reconcile independently. */
final case class GitWorkspace(dir: Path, remote: String, branch: String):
  /** Fetches the branch and checks out its head; returns the head commit. */
  def sync: IO[AgentError, CommitSha] =
    for
      cloned <- ZIO.attemptBlocking(Files.isDirectory(dir.resolve(".git"))).orElseFail(AgentError.Git(s"reading $dir"))
      _      <- (ZIO.attemptBlocking(Files.createDirectories(dir.getParent)).orElseFail(AgentError.Git(s"creating ${dir.getParent}")) *>
                  GitWorkspace.run(dir.getParent.nn, "clone", "--quiet", remote, dir.toString)).unless(cloned)
      _      <- git("fetch", "--quiet", "origin", branch)
      _      <- git("checkout", "--quiet", "--force", "--detach", s"origin/$branch")
      head   <- git("rev-parse", "HEAD")
      sha    <- ZIO.fromEither(CommitSha.from(head)).mapError(AgentError.Git(_))
    yield sha

  /** The email of the head commit's author. */
  def author: IO[AgentError, String] = git("log", "-1", "--format=%ae")

  /** The email of a commit's author. */
  def authorOf(commit: CommitSha): IO[AgentError, String] = git("log", "-1", "--format=%ae", commit.value)

  /** Whether `commit` is on the branch: the head or one of its ancestors. */
  def contains(commit: CommitSha, head: CommitSha): IO[AgentError, Boolean] =
    git("merge-base", "--is-ancestor", commit.value, head.value).as(true).catchSome:
      case AgentError.Git(detail) if detail.contains("exited 1") => ZIO.succeed(false)

  /** Checks out a commit (detached), e.g. to work on what an older reconcile planned. */
  def checkout(commit: CommitSha): IO[AgentError, Unit] = git("checkout", "--quiet", "--force", "--detach", commit.value).unit

  /** A file's content at a commit, or None if the file doesn't exist there. */
  def show(commit: CommitSha, path: String): IO[AgentError, Option[String]] =
    git("ls-tree", "--name-only", commit.value, "--", path).flatMap: listed =>
      ZIO.when(listed.nonEmpty)(git("show", s"${commit.value}:$path"))

  def git(args: String*): IO[AgentError, String] = GitWorkspace.run(dir, args*)

object GitWorkspace:
  def run(dir: Path, args: String*): IO[AgentError, String] =
    Process.run(dir, "git" +: args, 2.minutes).mapError(AgentError.Git(_))

/** Runs a command and returns its standard output, failing with its output when it exits non-zero. */
object Process:
  /** The timeout is real time, even when the agent runs on a faster clock (the local harness). */
  def run(dir: Path, command: Seq[String], timeout: Duration): IO[String, String] =
    ZIO.withClock(Clock.ClockLive):
      ZIO
        .attemptBlockingInterrupt:
          val process = ProcessBuilder(command*).directory(dir.toFile).redirectError(ProcessBuilder.Redirect.PIPE).start().nn
          val out     = String(process.getInputStream.nn.readAllBytes().nn).trim.nn
          val err     = String(process.getErrorStream.nn.readAllBytes().nn).trim.nn
          (process.waitFor(), out, err)
        .mapError(e => s"${command.head}: ${e.getMessage}")
        .timeoutFail(s"${command.mkString(" ")} timed out after $timeout")(timeout)
        .flatMap:
          case (0, out, _)      => ZIO.succeed(out)
          case (code, out, err) => ZIO.fail(s"${command.mkString(" ")} exited $code: ${(err + "\n" + out).trim.take(2000)}")
