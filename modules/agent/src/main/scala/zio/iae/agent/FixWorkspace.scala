package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*
import zio.schema.{Schema, derived}

/** Where the agent prepares a fix: a scratch clone of an env's workspace at the failing commit, so the reconciler's
  * checkout never moves. Changes leave it only as one `iae/fix-...` branch and a pull request, which still needs
  * review and passes through the policy like any other change.
  */
final case class FixWorkspace private (dir: Path, commit: CommitSha, base: String, prs: PullRequests, opened: Ref[Option[PullRequest]]):
  import FixWorkspace.*

  def writeFile(in: WriteInput): IO[String, Written] =
    for
      file <- ZIO.fromEither(FixWorkspace.resolve(dir, in.path))
      _    <- ZIO.attemptBlocking:
                Files.createDirectories(file.getParent)
                Files.writeString(file, in.content, UTF_8)
              .mapError(e => s"writing ${in.path}: ${e.getMessage}")
    yield Written(in.path, in.content.length)

  def openPullRequest(in: PullRequestInput): IO[String, Opened] =
    opened.get.flatMap:
      case Some(pr) => ZIO.fail(s"a pull request is already open for this failure: ${pr.url}")
      case None =>
        for
          changes <- git("status", "--porcelain")
          _       <- ZIO.fail("there are no changes to propose; write files first").when(changes.isEmpty)
          suffix  <- Random.nextIntBounded(100000)
          branch   = s"iae/fix-${commit.short}-$suffix"
          _       <- git("add", "-A")
          _       <- git("-c", "user.name=iae", "-c", "user.email=iae@users.noreply.github.com", "commit", "-q", "-m", in.title)
          url     <- prs.pushUrl
          _       <- GitWorkspace.run(dir, "push", "-q", url, s"HEAD:refs/heads/$branch").mapError(e => redact(e.message, url))
          pr      <- prs.open(branch, base, in.title, s"${in.body}\n\n---\nOpened by iae for a failure at `${commit.value}`.")
          _       <- opened.set(Some(pr))
        yield Opened(pr.number, pr.url, branch)

  private def git(args: String*): IO[String, String] = GitWorkspace.run(dir, args*).mapError(_.message)

object FixWorkspace:
  final case class WriteInput(path: String, content: String) derives Schema
  final case class Written(path: String, characters: Int) derives Schema
  final case class PullRequestInput(title: String, body: String) derives Schema
  final case class Opened(number: Int, url: String, branch: String) derives Schema

  /** A scratch clone of `source` at `commit`, deleted with the scope. */
  def make(source: GitWorkspace, commit: CommitSha, prs: PullRequests): ZIO[Scope, String, FixWorkspace] =
    for
      root   <- ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("iae-fix").nn).mapError(_.getMessage)): dir =>
                  ZIO.attemptBlocking(scala.sys.process.Process(Seq("rm", "-rf", dir.toString)).!).ignoreLogged
      dir     = root.resolve("repo").nn
      _      <- GitWorkspace.run(root, "clone", "-q", "--no-checkout", source.dir.toString, dir.toString).mapError(_.message)
      _      <- GitWorkspace.run(dir, "checkout", "-q", "--detach", commit.value).mapError(_.message)
      opened <- Ref.make(Option.empty[PullRequest])
    yield FixWorkspace(dir, commit, source.branch, prs, opened)

  /** A path inside the repository, never `.git` or outside it. */
  def resolve(dir: Path, path: String): Either[String, Path] =
    val file = dir.resolve(path.stripPrefix("/")).nn.normalize.nn
    if !file.startsWith(dir) || file.equals(dir) then Left(s"'$path' is outside the repository")
    else if dir.relativize(file).nn.iterator.nn.next.toString == ".git" then Left(s"'$path' is inside .git")
    else Right(file)

  def redact(text: String, url: String): String =
    val token = url.replaceFirst("^https://[^:]+:([^@]+)@.*$", "$1")
    if token == url then text else text.replace(token, "***")

/** Pull requests against a local repository, for the harness and tests: pushes go to `remote` and pull requests are
  * recorded.
  */
final case class LocalPullRequests(remote: String, opened: Ref[Chunk[(PullRequest, String, String, String)]]) extends PullRequests:
  def pushUrl: IO[String, String] = ZIO.succeed(remote)

  def open(branch: String, base: String, title: String, body: String): IO[String, PullRequest] =
    opened.modify: all =>
      val pr = PullRequest(all.size + 1, s"local://pull/${all.size + 1}")
      (pr, all :+ (pr, branch, title, body))

object LocalPullRequests:
  def make(remote: String): UIO[LocalPullRequests] = Ref.make(Chunk.empty[(PullRequest, String, String, String)]).map(LocalPullRequests(remote, _))
