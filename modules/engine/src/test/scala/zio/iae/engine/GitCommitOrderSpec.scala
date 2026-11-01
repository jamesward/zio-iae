package zio.iae.engine

import java.nio.file.{Files, Path}
import zio.*
import zio.iae.CommitSha
import zio.test.*

object GitCommitOrderSpec extends ZIOSpecDefault:
  def git(dir: Path, args: String*): Task[String] =
    ZIO.attemptBlocking:
      val process = ProcessBuilder(("git" +: args)*).directory(dir.toFile).redirectErrorStream(true).start().nn
      val out     = String(process.getInputStream.nn.readAllBytes().nn).trim
      if process.waitFor() != 0 then throw IllegalStateException(s"git ${args.mkString(" ")}: $out")
      out

  def commit(dir: Path, message: String): Task[CommitSha] =
    git(dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "--allow-empty", "-q", "-m", message) *>
      git(dir, "rev-parse", "HEAD").flatMap(s => ZIO.fromEither(CommitSha.from(s)).mapError(IllegalStateException(_)))

  val repo: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("iae-git").nn)): dir =>
      ZIO.attemptBlocking(scala.sys.process.Process(Seq("rm", "-rf", dir.toString)).!).ignoreLogged

  def spec = suite("GitCommitOrder")(
    test("orders commits by ancestry and rejects unknown ones"):
      for
        dir     <- repo
        _       <- git(dir, "init", "-q")
        first   <- commit(dir, "first")
        second  <- commit(dir, "second")
        order    = GitCommitOrder(dir)
        forward <- order.isAncestor(first, second)
        back    <- order.isAncestor(second, first)
        unknown <- order.isAncestor(CommitSha.from("f" * 40).toOption.get, second).either
      yield assertTrue(forward, !back, unknown.isLeft)
  )
