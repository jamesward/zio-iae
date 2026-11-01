package zio.iae.aws

import java.time.LocalDate
import scala.util.Try
import zio.iae.*

/** The tags that mark a resource as managed by zio-iae. Tags are the ownership record: there is no state file. */
enum TagKey derives CanEqual:
  case Managed, App, Env, Id, Kind, Spec, Commit, Orphaned

  def wire: String = this match
    case Managed  => "iae:managed"
    case App      => "iae:app"
    case Env      => "iae:env"
    case Id       => "iae:id"
    case Kind     => "iae:kind"
    case Spec     => "iae:spec-hash"
    case Commit   => "iae:commit"
    case Orphaned => "iae:orphaned"

object ManagedTags:
  /** The tags that say what a resource is and which spec it was provisioned from. Plans compare these. */
  def identity(address: Address, spec: SpecHash): Map[String, String] =
    Map(
      TagKey.Managed.wire -> "true",
      TagKey.App.wire     -> address.app.value,
      TagKey.Env.wire     -> address.env.value,
      TagKey.Id.wire      -> address.id.value,
      TagKey.Kind.wire    -> address.kind.toString,
      TagKey.Spec.wire    -> spec.value,
    )

  /** The tags after converging to a spec: the user's own tags kept, ours replaced, the orphan mark removed, and the
    * commit updated when the apply is for one.
    */
  def converged(current: Map[String, String], address: Address, spec: SpecHash, context: ApplyContext): Map[String, String] =
    val previousCommit = current.get(TagKey.Commit.wire)
    current.filterNot((k, _) => isManaged(k)) ++ identity(address, spec) ++
      context.commit.map(_.value).orElse(previousCommit).map(TagKey.Commit.wire -> _)

  def orphaned(current: Map[String, String], since: LocalDate, context: ApplyContext): Map[String, String] =
    current + (TagKey.Orphaned.wire -> since.toString) ++ context.commit.map(c => TagKey.Commit.wire -> c.value)

  def specHash(tags: Map[String, String]): Option[SpecHash]         = tags.get(TagKey.Spec.wire).flatMap(SpecHash.from)
  def commit(tags: Map[String, String]): Option[CommitSha]          = tags.get(TagKey.Commit.wire).flatMap(CommitSha.from(_).toOption)
  def orphanedSince(tags: Map[String, String]): Option[LocalDate]   = tags.get(TagKey.Orphaned.wire).flatMap(d => Try(LocalDate.parse(d).nn).toOption)

  def isManaged(key: String): Boolean = TagKey.values.exists(_.wire == key)

  /** What the identity tags differ in, for plans. */
  def differences(current: Map[String, String], desired: Map[String, String]): List[FieldChange] =
    val changed = desired.toList.sorted.collect:
      case (key, value) if !current.get(key).contains(value) => FieldChange(s"tag $key", current.get(key).fold("(none)")(v => v), value)
    val orphan = current.get(TagKey.Orphaned.wire).map(since => FieldChange("orphaned", since, "(none)"))
    changed ++ orphan.toList
