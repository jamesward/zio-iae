package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.{AwsError, TaggedResource, TaggingClient}

/** Finds every resource an app owns in an env from its tags, across kinds, with the Resource Groups Tagging API. */
final case class TaggingDiscovery(tagging: TaggingClient) extends Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]] =
    val filters = Map(TagKey.Managed.wire -> "true", TagKey.App.wire -> app.value, TagKey.Env.wire -> env.value)
    tagging
      .resources(filters)
      .runCollect
      .mapError(failed(app, env))
      .flatMap: found =>
        ZIO.foreach(found.toList)(r => discovered(app, env, r)).map(_.flatten)

  private def discovered(app: AppName, env: EnvName, resource: TaggedResource): UIO[Option[Discovered]] =
    val parsed =
      for
        kindTag <- resource.tags.get(TagKey.Kind.wire).toRight("no iae:kind tag")
        kind    <- ResourceKind.values.find(_.toString == kindTag).toRight(s"unknown kind '$kindTag'")
        idTag   <- resource.tags.get(TagKey.Id.wire).toRight("no iae:id tag")
        id      <- LogicalId.from(idTag).left.map(_.message)
      yield Discovered(Address(app, env, kind, id), ManagedTags.commit(resource.tags), ManagedTags.orphanedSince(resource.tags))
    parsed match
      case Right(d)     => ZIO.some(d)
      case Left(reason) => ZIO.logWarning(s"ignoring ${resource.arn}: tagged as managed by ${app.value}/${env.value} but $reason").as(None)

  private def failed(app: AppName, env: EnvName)(error: AwsError): ProviderError =
    ProviderError(Address(app, env, ResourceKind.Bucket, LogicalId("discovery")), "discover", error.message)
