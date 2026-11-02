package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Secret names: `iae/<app>/<env>/<id>`. */
object SecretNaming:
  def physical(address: Address): String = s"iae/${address.app.value}/${address.env.value}/${address.id.value}"

  /** Secrets Manager appends six random characters to a secret's ARN. */
  def arnPattern(context: AwsContext, address: Address): String =
    s"arn:aws:secretsmanager:${context.region.value}:${context.account.value}:secret:${physical(address)}-??????"

final case class SecretProvider(secrets: SecretsClient) extends Provider[SecretSpec, SecretDescription]:
  val kind: ResourceKind = ResourceKind.Secret

  def observe(address: Address): IO[ProviderError, Option[SecretDescription]] =
    secrets.describe(SecretNaming.physical(address)).mapError(failed(address, "observe"))

  def diff(address: Address, desired: SecretSpec, observed: Option[SecretDescription]): Option[Change[SecretSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val description = Option.when(current.description.getOrElse("") != desired.description):
          FieldChange("description", current.description.getOrElse("(none)"), desired.description)
        val tags = ManagedTags.differences(current.tags, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Secret(desired))))
        NonEmptyChunk.fromIterableOption(description.toList ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[SecretSpec], observed: Option[SecretDescription]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Low, Set.empty)
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.Critical, Set(ChangeFlag.DataLoss))
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  def applyChange(change: Change[SecretSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    val name    = SecretNaming.physical(address)
    change match
      case Change.Create(_, spec) =>
        secrets
          .create(name, spec.description, ManagedTags.converged(Map.empty, address, SpecHash.of(ResourceSpec.Secret(spec)), context))
          .catchSome { case e if e.errorCode.contains("ResourceExistsException") => ZIO.unit }
          .mapError(failed(address, "create")) *> converge(address, spec, context)
      case Change.Update(_, spec, _) => converge(address, spec, context)
      case Change.Orphan(_, since) =>
        for
          current <- secrets.describe(name).mapError(failed(address, "observe")).someOrFail(ProviderError(address, "observe", s"secret $name does not exist"))
          _       <- secrets.tag(name, ManagedTags.orphaned(current.tags, since, context)).mapError(failed(address, "tag as orphaned"))
        yield ()
      case Change.Delete(_) => secrets.delete(name).mapError(failed(address, "delete"))
      case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))

  private def converge(address: Address, spec: SecretSpec, context: ApplyContext): IO[ProviderError, Unit] =
    val name = SecretNaming.physical(address)
    for
      current <- secrets.describe(name).mapError(failed(address, "observe")).someOrFail(ProviderError(address, "observe", s"secret $name does not exist"))
      _       <- secrets.updateDescription(name, spec.description).mapError(failed(address, "set description")).when(current.description.getOrElse("") != spec.description)
      desired  = ManagedTags.converged(current.tags, address, SpecHash.of(ResourceSpec.Secret(spec)), context)
      _       <- secrets.tag(name, desired).mapError(failed(address, "tag"))
      _       <- secrets.untag(name, current.tags.keySet -- desired.keySet).mapError(failed(address, "untag"))
    yield ()

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

final case class SecretDriver(provider: SecretProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Secret

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Secret(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Secret(_)), assessment.risk, assessment.flags ++ Drifted.flags(change))
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a secret spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[SecretSpec]] = change match
      case Change.Create(address, ResourceSpec.Secret(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Secret(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                             => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                    => Right(Change.Delete(address))
      case other                                                     => Left(ProviderError(other.address, "apply", "expected a secret change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))

final case class AwsSecret[I](id: LogicalId, secrets: SecretsClient, name: String) extends SecretReader[I]:
  def read: IO[SecretError, SecretValue] =
    secrets.value(name).mapError(e => SecretError.Unavailable(e.message)).someOrFail(SecretError.NotSet(id)).map(SecretValue(_))
