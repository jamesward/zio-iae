package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

final case class ObservedRole(name: RoleName, trust: Option[TrustDocument], permissions: Option[PolicyDocument], tags: Map[String, String])

/** Runtime roles: `iae-<app>-<env>` under the `/iae/` path, with one inline policy generated from the app's grants. */
final case class RoleProvider(iam: IamClient, buckets: BucketNaming, context: AwsContext) extends Provider[RoleSpec, ObservedRole]:
  import RoleProvider.*

  val kind: ResourceKind = ResourceKind.Role

  def observe(address: Address): IO[ProviderError, Option[ObservedRole]] =
    nameOf(address).flatMap: name =>
      iam.getRole(name).mapError(failed(address, "observe")).flatMap:
        case None => ZIO.none
        case Some(role) =>
          for
            policy <- iam.getInlinePolicy(name, policyName).mapError(failed(address, "read policy"))
            tags   <- iam.roleTags(name).mapError(failed(address, "read tags"))
          yield Some(ObservedRole(name, role.trustPolicy.flatMap(IamPolicy.parseTrust), policy.flatMap(IamPolicy.parsePermissions), tags))

  def diff(address: Address, desired: RoleSpec, observed: Option[ObservedRole]): Option[Change[RoleSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        documents(address, desired).toOption.flatMap: (trust, permissions) =>
          val trustChange = Option.when(!current.trust.map(_.services).contains(trust.services)):
            FieldChange("trusted services", current.trust.fold("(unreadable)")(_.services.toList.sorted.mkString(",")), trust.services.toList.sorted.mkString(","))
          val before  = current.permissions.fold(Set.empty[(String, String)])(_.permissions)
          val added   = permissions.permissions -- before
          val removed = before -- permissions.permissions
          val permissionChanges =
            added.toList.sorted.map((a, r) => FieldChange("permission", "(none)", s"$a on $r")) ++
              removed.toList.sorted.map((a, r) => FieldChange("permission", s"$a on $r", "(none)")) ++
              Option.when(current.permissions.isEmpty && permissions.permissions.isEmpty)(FieldChange("policy", "(unreadable)", "(empty)")).toList
          val tags = ManagedTags.differences(current.tags, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Role(desired))))
          NonEmptyChunk.fromIterableOption(trustChange.toList ++ permissionChanges ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[RoleSpec], observed: Option[ObservedRole]): Assessment = change match
    case Change.Create(_, _) => Assessment(Risk.Low, Set(ChangeFlag.IamWidening))
    case Change.Update(_, _, fields) =>
      val widening = fields.exists(f => (f.field == "permission" && f.from == "(none)") || f.field == "trusted services")
      if widening then Assessment(Risk.High, Set(ChangeFlag.IamWidening)) else Assessment(Risk.Medium, Set.empty)
    case Change.Orphan(_, _) => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)    => Assessment(Risk.High, Set.empty)
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  def applyChange(change: Change[RoleSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    nameOf(address).flatMap: name =>
      change match
        case Change.Create(_, spec) =>
          for
            docs <- ZIO.fromEither(documents(address, spec)).mapError(e => ProviderError(address, "render policy", e.message))
            tags  = ManagedTags.converged(Map.empty, address, SpecHash.of(ResourceSpec.Role(spec)), context)
            _ <- iam.createRole(name, path, docs._1.toJson, tags).catchSome {
                   case e if e.errorCode.contains("EntityAlreadyExists") => ZIO.unit
                 }.mapError(failed(address, "create"))
            _ <- converge(address, name, spec, context)
          yield ()
        case Change.Update(_, spec, _) => converge(address, name, spec, context)
        case Change.Orphan(_, since) =>
          iam.roleTags(name).flatMap(tags => iam.tagRole(name, ManagedTags.orphaned(tags, since, context))).mapError(failed(address, "tag as orphaned"))
        case Change.Delete(_) =>
          (iam.deleteInlinePolicy(name, policyName) *> iam.deleteRole(name)).mapError(failed(address, "delete"))
        case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))

  private def converge(address: Address, name: RoleName, spec: RoleSpec, context: ApplyContext): IO[ProviderError, Unit] =
    for
      docs   <- ZIO.fromEither(documents(address, spec)).mapError(e => ProviderError(address, "render policy", e.message))
      _      <- iam.updateTrustPolicy(name, docs._1.toJson).mapError(failed(address, "set trust policy"))
      _      <- iam.putInlinePolicy(name, policyName, docs._2.toJson).mapError(failed(address, "set policy"))
      tags   <- iam.roleTags(name).mapError(failed(address, "read tags"))
      desired = ManagedTags.converged(tags, address, SpecHash.of(ResourceSpec.Role(spec)), context)
      _      <- iam.tagRole(name, desired).mapError(failed(address, "tag"))
      _      <- iam.untagRole(name, tags.keySet -- desired.keySet).mapError(failed(address, "untag"))
    yield ()

  private def documents(address: Address, spec: RoleSpec): Either[InfraError, (TrustDocument, PolicyDocument)] =
    IamPolicy.permissions(address.app, address.env, spec, buckets, context).map(IamPolicy.trust(spec) -> _)

  private def nameOf(address: Address): IO[ProviderError, RoleName] =
    ZIO.fromEither(RoleProvider.roleName(address.app, address.env)).mapError(e => ProviderError(address, "name", e))

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

object RoleProvider:
  val path       = "/iae/"
  val policyName = "iae-grants"

  def roleName(app: AppName, env: EnvName): Either[String, RoleName] = RoleName.from(s"iae-${app.value}-${env.value}")

final case class RoleDriver(provider: RoleProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Role

  override def deletion(address: Address): IO[ProviderError, Option[PlannedChange]] =
    provider.observe(address).map(_.map: observed =>
      val assessment = provider.assess(Change.Delete(address), Some(observed))
      PlannedChange(Change.Delete(address), assessment.risk, assessment.flags))

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Role(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Role(_)), assessment.risk, assessment.flags ++ Drifted.flags(change))
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a role spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[RoleSpec]] = change match
      case Change.Create(address, ResourceSpec.Role(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Role(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                           => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                  => Right(Change.Delete(address))
      case other                                                   => Left(ProviderError(other.address, "apply", "expected a role change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))

/** IAM roles aren't covered by the tagging API on AWS, so roles are found by path and tags. */
final case class RoleDiscovery(iam: IamClient) extends Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]] =
    val address = Address(app, env, ResourceKind.Role, Manifest.runtimeRole)
    iam
      .listRoles(RoleProvider.path)
      .mapZIO(role => iam.roleTags(role.name).map(role -> _))
      .collect:
        case (_, tags) if tags.get(TagKey.App.wire).contains(app.value) && tags.get(TagKey.Env.wire).contains(env.value) && tags.get(TagKey.Managed.wire).contains("true") =>
          tags
      .runCollect
      .mapBoth(
        e => ProviderError(address, "discover roles", e.message),
        _.toList.flatMap: tags =>
          tags.get(TagKey.Id.wire).flatMap(LogicalId.from(_).toOption).map: id =>
            Discovered(Address(app, env, ResourceKind.Role, id), ManagedTags.commit(tags), ManagedTags.orphanedSince(tags)),
      )

final case class CompositeDiscovery(all: List[Discovery]) extends Discovery:
  def managed(app: AppName, env: EnvName): IO[ProviderError, List[Discovered]] =
    ZIO.foreach(all)(_.managed(app, env)).map(_.flatten.distinctBy(_.address))
