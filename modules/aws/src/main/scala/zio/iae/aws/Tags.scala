package zio.iae.aws

import zio.iae.*

/** The tags that mark a resource as managed by zio-iae. Tags are the ownership record: there is no state file. */
enum TagKey derives CanEqual:
  case Managed, App, Env, Id, Kind, Spec

  def wire: String = this match
    case Managed => "iae:managed"
    case App     => "iae:app"
    case Env     => "iae:env"
    case Id      => "iae:id"
    case Kind    => "iae:kind"
    case Spec    => "iae:spec-hash"

object ManagedTags:
  def of(address: Address, spec: SpecHash): Map[String, String] =
    Map(
      TagKey.Managed.wire -> "true",
      TagKey.App.wire     -> address.app.value,
      TagKey.Env.wire     -> address.env.value,
      TagKey.Id.wire      -> address.id.value,
      TagKey.Kind.wire    -> address.kind.toString,
      TagKey.Spec.wire    -> spec.value,
    )

  def specHash(tags: Map[String, String]): Option[SpecHash] = tags.get(TagKey.Spec.wire).flatMap(SpecHash.from)

  def isManaged(key: String): Boolean = TagKey.values.exists(_.wire == key)
