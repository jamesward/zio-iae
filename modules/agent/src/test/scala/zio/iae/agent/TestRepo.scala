package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import zio.*
import zio.iae.*

/** A bare "remote" plus an author's clone, to push commits the agent then fetches. */
final case class TestRepo(root: Path, remote: Path, authorClone: Path):
  def commit(files: Map[String, String], message: String): IO[String, CommitSha] =
    for
      _ <- ZIO.foreachDiscard(files): (name, content) =>
             ZIO.attemptBlocking:
               val file = authorClone.resolve(name).nn
               Files.createDirectories(file.getParent)
               Files.writeString(file, content, UTF_8)
             .mapError(_.getMessage)
      _   <- git("add", "-A")
      _   <- git("-c", "user.name=Dev", "-c", "user.email=dev@example.com", "commit", "-q", "-m", message)
      _   <- git("push", "-q", "origin", "HEAD:main")
      sha <- git("rev-parse", "HEAD").flatMap(s => ZIO.fromEither(CommitSha.from(s)))
    yield sha

  def workspace(env: EnvName): GitWorkspace = GitWorkspace(root.resolve("agent").nn.resolve(env.value).nn, remote.toString, "main")

  private def git(args: String*): IO[String, String] = Process.run(authorClone, "git" +: args, 1.minute)

object TestRepo:
  val make: ZIO[Scope, String, TestRepo] =
    for
      root   <- ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("iae-repo").nn).mapError(_.getMessage)): dir =>
                  ZIO.attemptBlocking(scala.sys.process.Process(Seq("rm", "-rf", dir.toString)).!).ignoreLogged
      remote  = root.resolve("remote.git").nn
      clone   = root.resolve("author").nn
      _      <- Process.run(root, Seq("git", "init", "-q", "--bare", "-b", "main", remote.toString), 1.minute)
      _      <- Process.run(root, Seq("git", "clone", "-q", remote.toString, clone.toString), 1.minute)
      _      <- Process.run(clone, Seq("git", "checkout", "-q", "-b", "main"), 1.minute)
    yield TestRepo(root, remote, clone)
