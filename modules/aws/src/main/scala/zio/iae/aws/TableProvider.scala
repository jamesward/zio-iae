package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Table names: `<app>-<env>-<id>`. */
object TableNaming:
  def physical(address: Address): String = s"${address.app.value}-${address.env.value}-${address.id.value}"

  def arn(context: AwsContext, address: Address): String =
    s"arn:aws:dynamodb:${context.region.value}:${context.account.value}:table/${physical(address)}"

final case class ObservedTable(table: DynamoTable, tags: Map[String, String])

final case class TableProvider(dynamo: DynamoClient) extends Provider[TableSpec, ObservedTable]:
  val kind: ResourceKind = ResourceKind.Table

  def observe(address: Address): IO[ProviderError, Option[ObservedTable]] =
    dynamo
      .describe(TableNaming.physical(address))
      .flatMap(ZIO.foreach(_)(t => dynamo.tags(t.arn).map(ObservedTable(t, _))))
      .mapError(failed(address, "observe"))

  def diff(address: Address, desired: TableSpec, observed: Option[ObservedTable]): Option[Change[TableSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val billing = Option.when(!current.table.billingMode.forall(_ == "PAY_PER_REQUEST")):
          FieldChange("billing", current.table.billingMode.getOrElse("(none)"), desired.billing.toString)
        val tags = ManagedTags.differences(current.tags, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Table(desired))))
        NonEmptyChunk.fromIterableOption(billing.toList ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[TableSpec], observed: Option[ObservedTable]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Medium, Set.empty)
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.Critical, Set(ChangeFlag.DataLoss))
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  def applyChange(change: Change[TableSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    val name    = TableNaming.physical(address)
    change match
      case Change.Create(_, spec) =>
        for
          _ <- dynamo
                 .create(name, ManagedTags.converged(Map.empty, address, SpecHash.of(ResourceSpec.Table(spec)), context))
                 .catchSome { case e if e.errorCode.contains("ResourceInUseException") => ZIO.unit }
                 .mapError(failed(address, "create"))
          _ <- dynamo.awaitActive(name).mapError(failed(address, "wait for table"))
          _ <- converge(address, spec, context)
        yield ()
      case Change.Update(_, spec, _) => converge(address, spec, context)
      case Change.Orphan(_, since) =>
        existing(address).flatMap: current =>
          dynamo.tag(current.table.arn, ManagedTags.orphaned(current.tags, since, context)).mapError(failed(address, "tag as orphaned"))
      case Change.Delete(_) => dynamo.delete(name).mapError(failed(address, "delete"))
      case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))

  private def converge(address: Address, spec: TableSpec, context: ApplyContext): IO[ProviderError, Unit] =
    for
      current <- existing(address)
      desired  = ManagedTags.converged(current.tags, address, SpecHash.of(ResourceSpec.Table(spec)), context)
      _       <- dynamo.tag(current.table.arn, desired).mapError(failed(address, "tag"))
      _       <- dynamo.untag(current.table.arn, current.tags.keySet -- desired.keySet).mapError(failed(address, "untag"))
    yield ()

  private def existing(address: Address): IO[ProviderError, ObservedTable] =
    observe(address).someOrFail(ProviderError(address, "observe", s"table ${TableNaming.physical(address)} does not exist"))

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

final case class TableDriver(provider: TableProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Table

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Table(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Table(_)), assessment.risk, assessment.flags ++ Drifted.flags(change))
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a table spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[TableSpec]] = change match
      case Change.Create(address, ResourceSpec.Table(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Table(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                            => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                   => Right(Change.Delete(address))
      case other                                                    => Left(ProviderError(other.address, "apply", "expected a table change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))

/** A typed key-value `Table` on DynamoDB: keys and values are JSON from the table's declared schemas. */
final case class DynamoKeyValue[I <: Table.Id[?, ?]](id: I, dynamo: DynamoClient, name: String) extends Table[I]:
  def get[K, V](key: K)(using ev: I <:< Table.Id[K, V]): IO[TableError, Option[V]] =
    dynamo
      .get(name, Table.encode(ev(id).keySchema, key))
      .mapError(e => TableError.Unavailable(e.message))
      .flatMap(ZIO.foreach(_)(json => ZIO.fromEither(Table.decode(ev(id).valueSchema, json)).mapError(TableError.Undecodable(_))))

  def put[K, V](key: K, value: V)(using ev: I <:< Table.Id[K, V]): IO[TableError, Unit] =
    dynamo.put(name, Table.encode(ev(id).keySchema, key), Table.encode(ev(id).valueSchema, value)).mapError(e => TableError.Unavailable(e.message))

  def delete[K, V](key: K)(using ev: I <:< Table.Id[K, V]): IO[TableError, Unit] =
    dynamo.deleteItem(name, Table.encode(ev(id).keySchema, key)).mapError(e => TableError.Unavailable(e.message))
