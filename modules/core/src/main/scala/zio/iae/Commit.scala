package zio.iae

import zio.schema.Schema

/** A full git commit id. Resources record the commit that last changed them (`iae:commit`). */
opaque type CommitSha = String

object CommitSha:
  private val pattern = "[0-9a-f]{40}".r

  def from(value: String): Either[String, CommitSha] =
    Either.cond(pattern.matches(value), value, s"'$value' is not a full 40-character commit id")

  extension (sha: CommitSha)
    def value: String = sha
    def short: String = sha.take(7)

  given CanEqual[CommitSha, CommitSha] = CanEqual.derived
  given Schema[CommitSha]              = Schema[String].transformOrFail(from, s => Right(s))

/** What an apply is doing on behalf of: the commit being deployed, if any (local auto-provisioning has none). */
final case class ApplyContext(commit: Option[CommitSha])

object ApplyContext:
  val local: ApplyContext = ApplyContext(None)
