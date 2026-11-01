package zio.iae.engine

import java.nio.file.Path
import zio.*
import zio.iae.CommitSha

/** Answers whether one commit is an ancestor of another. Backs the commit guard. */
trait CommitOrder:
  def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean]

/** Asks git (`git merge-base --is-ancestor`) in a local clone. */
final case class GitCommitOrder(repository: Path) extends CommitOrder:
  def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] =
    ZIO
      .attemptBlocking:
        val process = ProcessBuilder("git", "merge-base", "--is-ancestor", ancestor.value, descendant.value)
          .directory(repository.toFile)
          .redirectErrorStream(true)
          .start()
          .nn
        val output = String(process.getInputStream.nn.readAllBytes().nn)
        (process.waitFor(), output)
      .mapError(e => EngineError.CommitOrder(s"running git: ${e.getMessage}"))
      .flatMap:
        case (0, _)      => ZIO.succeed(true)
        case (1, _)      => ZIO.succeed(false)
        case (code, out) => ZIO.fail(EngineError.CommitOrder(s"git merge-base exited $code: ${out.trim}"))
