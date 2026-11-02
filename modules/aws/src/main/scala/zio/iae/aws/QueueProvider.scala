package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

/** Queue names: `<app>-<env>-<id>`, and `<app>-<env>-<id>-dlq` for its dead-letter queue. */
object QueueNaming:
  private val pattern = "[A-Za-z0-9_-]{1,80}".r

  def physical(address: Address): Either[InfraError, String] =
    val name = s"${address.app.value}-${address.env.value}-${address.id.value}"
    Either.cond(pattern.matches(s"$name-dlq"), name, InfraError.InvalidConfig(s"queue name '$name' is too long"))

  def deadLetter(address: Address): Either[InfraError, String] = physical(address).map(_ + "-dlq")

  def arn(context: AwsContext, name: String): String = s"arn:aws:sqs:${context.region.value}:${context.account.value}:$name"

/** Where the engine runs: needed to build ARNs that include the region and account. */
final case class AwsContext(region: Region, account: AccountId)

object AwsContext:
  val layer: ZLayer[AwsConfig & StsClient, AwsError, AwsContext] =
    ZLayer:
      for
        config  <- ZIO.service[AwsConfig]
        account <- ZIO.serviceWithZIO[StsClient](_.accountId)
      yield AwsContext(config.region, account)

final case class ObservedQueue(url: QueueUrl, attributes: Map[String, String], tags: Map[String, String], deadLetterExists: Boolean)

final case class QueueProvider(sqs: SqsClient) extends Provider[QueueSpec, ObservedQueue]:
  import QueueProvider.*

  val kind: ResourceKind = ResourceKind.Queue

  def observe(address: Address): IO[ProviderError, Option[ObservedQueue]] =
    for
      name <- named(address, QueueNaming.physical)
      dlq  <- named(address, QueueNaming.deadLetter)
      url  <- sqs.queueUrl(name).mapError(failed(address, "observe"))
      observed <- ZIO.foreach(url): u =>
                    for
                      attributes <- sqs.attributes(u).mapError(failed(address, "read attributes"))
                      tags       <- sqs.tags(u).mapError(failed(address, "read tags"))
                      dlqUrl     <- sqs.queueUrl(dlq).mapError(failed(address, "observe dead-letter queue"))
                    yield ObservedQueue(u, attributes, tags, dlqUrl.isDefined)
    yield observed

  def diff(address: Address, desired: QueueSpec, observed: Option[ObservedQueue]): Option[Change[QueueSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val settings = List(
          ("visibility timeout (s)", current.attributes.get(VisibilityTimeout), desired.visibilityTimeout.toSeconds.toString),
          ("retention (s)", current.attributes.get(Retention), desired.retention.toSeconds.toString),
          ("dead-letter max receives", current.attributes.get(Redrive).flatMap(maxReceives).map(_.toString).filter(_ => current.deadLetterExists), desired.deadLetter.fold("(none)")(_.maxReceives.toString)),
        ).collect:
          case (field, from, to) if !from.contains(to) && !(from.isEmpty && to == "(none)") => FieldChange(field, from.getOrElse("(none)"), to)
        val tags = ManagedTags.differences(current.tags, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Queue(desired))))
        NonEmptyChunk.fromIterableOption(settings ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[QueueSpec], observed: Option[ObservedQueue]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Medium, Set.empty)
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.Critical, Set(ChangeFlag.DataLoss))
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  def applyChange(change: Change[QueueSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    change match
      case Change.Create(_, spec)    => converge(address, spec, context)
      case Change.Update(_, spec, _) => converge(address, spec, context)
      case Change.Orphan(_, since) =>
        for
          url  <- existing(address)
          tags <- sqs.tags(url).mapError(failed(address, "read tags"))
          _    <- sqs.tag(url, ManagedTags.orphaned(tags, since, context)).mapError(failed(address, "tag as orphaned"))
        yield ()
      case Change.Delete(_) =>
        for
          name <- named(address, QueueNaming.physical)
          dlq  <- named(address, QueueNaming.deadLetter)
          urls <- ZIO.foreach(List(name, dlq))(sqs.queueUrl).mapError(failed(address, "observe"))
          _    <- ZIO.foreachDiscard(urls.flatten)(sqs.deleteQueue).mapError(failed(address, "delete"))
        yield ()
      case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))

  /** Creates or updates the dead-letter queue first, so the main queue's redrive policy can point at it. */
  private def converge(address: Address, spec: QueueSpec, context: ApplyContext): IO[ProviderError, Unit] =
    for
      _      <- ZIO.fromEither(validate(spec)).mapError(ProviderError(address, "validate", _))
      name   <- named(address, QueueNaming.physical)
      dlqArn <- ZIO.foreach(spec.deadLetter)(_ => deadLetterQueue(address))
      attributes = Map(VisibilityTimeout -> spec.visibilityTimeout.toSeconds.toString, Retention -> spec.retention.toSeconds.toString) ++
                     dlqArn.zip(spec.deadLetter).map((arn, dl) => Redrive -> AwsJsonClient.encode(RedrivePolicy(arn, dl.maxReceives)))
      url <- sqs.queueUrl(name).mapError(failed(address, "observe")).someOrElseZIO(sqs.createQueue(name, attributes, Map.empty).mapError(failed(address, "create")))
      current <- sqs.attributes(url).mapError(failed(address, "read attributes"))
      removeRedrive = Option.when(spec.deadLetter.isEmpty && current.contains(Redrive))(Redrive -> "")
      _       <- sqs.setAttributes(url, attributes ++ removeRedrive.toList).mapError(failed(address, "set attributes"))
      tags    <- sqs.tags(url).mapError(failed(address, "read tags"))
      desired  = ManagedTags.converged(tags, address, SpecHash.of(ResourceSpec.Queue(spec)), context)
      _       <- sqs.tag(url, desired).mapError(failed(address, "tag"))
      _       <- sqs.untag(url, tags.keySet -- desired.keySet).mapError(failed(address, "untag"))
    yield ()

  private def deadLetterQueue(address: Address): IO[ProviderError, String] =
    for
      name <- named(address, QueueNaming.deadLetter)
      url  <- sqs.queueUrl(name).mapError(failed(address, "observe dead-letter queue"))
                .someOrElseZIO(sqs.createQueue(name, Map(Retention -> maxRetention.toString), Map.empty).mapError(failed(address, "create dead-letter queue")))
      attributes <- sqs.attributes(url).mapError(failed(address, "read dead-letter queue"))
      arn        <- ZIO.fromOption(attributes.get("QueueArn")).orElseFail(ProviderError(address, "read dead-letter queue", "no QueueArn"))
    yield arn

  private def existing(address: Address): IO[ProviderError, QueueUrl] =
    named(address, QueueNaming.physical).flatMap(name =>
      sqs.queueUrl(name).mapError(failed(address, "observe")).someOrFail(ProviderError(address, "observe", s"queue $name does not exist"))
    )

  private def named(address: Address, f: Address => Either[InfraError, String]): IO[ProviderError, String] =
    ZIO.fromEither(f(address)).mapError(e => ProviderError(address, "name", e.message))

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

object QueueProvider:
  val VisibilityTimeout = "VisibilityTimeout"
  val Retention         = "MessageRetentionPeriod"
  val Redrive           = "RedrivePolicy"
  val maxRetention      = 1209600

  private final case class RedrivePolicy(@fieldName("deadLetterTargetArn") target: String, @fieldName("maxReceiveCount") maxReceiveCount: Int) derives Schema

  /** AWS returns `maxReceiveCount` as a number or a string, so it is read leniently. */
  private val maxReceivesPattern = "\"maxReceiveCount\"\\s*:\\s*\"?(\\d+)".r

  def maxReceives(redrivePolicy: String): Option[Int] =
    maxReceivesPattern.findFirstMatchIn(redrivePolicy).flatMap(_.group(1).toIntOption)

  def validate(spec: QueueSpec): Either[String, Unit] =
    val visibility = spec.visibilityTimeout.toSeconds
    val retention  = spec.retention.toSeconds
    if visibility < 0 || visibility > 43200 then Left("visibility timeout must be between 0 seconds and 12 hours")
    else if retention < 60 || retention > maxRetention then Left("retention must be between 1 minute and 14 days")
    else spec.deadLetter.filter(d => d.maxReceives < 1 || d.maxReceives > 1000).fold(Right(()))(_ => Left("dead-letter max receives must be 1-1000"))

final case class QueueDriver(provider: QueueProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Queue

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Queue(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Queue(_)), assessment.risk, assessment.flags)
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a queue spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[QueueSpec]] = change match
      case Change.Create(address, ResourceSpec.Queue(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Queue(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                            => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                   => Right(Change.Delete(address))
      case other                                                    => Left(ProviderError(other.address, "apply", "expected a queue change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))

/** A `MessageQueue` backed by SQS; messages are JSON from the queue's declared message schema. */
final case class SqsQueue[I <: MessageQueue.Id[?]](id: I, sqs: SqsClient, url: QueueUrl) extends MessageQueue[I]:
  def send[M](message: M)(using ev: I <:< MessageQueue.Id[M]): IO[QueueError, Unit] =
    sqs.send(url, MessageQueue.encode(ev(id).schema, message)).mapBoth(e => QueueError.Unavailable(e.message), _ => ())

  def receive[M](max: Int, wait: Duration)(using ev: I <:< MessageQueue.Id[M]): IO[QueueError, Chunk[Received[M]]] =
    sqs
      .receive(url, max.max(1).min(10), wait.toSeconds.toInt.max(0).min(20))
      .mapError(e => QueueError.Unavailable(e.message))
      .flatMap: messages =>
        ZIO
          .foreach(messages): m =>
            MessageQueue.decode(ev(id).schema, m.body) match
              case Right(message) => ZIO.some(Received(message, Receipt(m.receipt), m.receiveCount.getOrElse(1)))
              case Left(error)    => ZIO.logWarning(s"undecodable message ${m.id} on ${id.logicalId.value}: $error").as(None)
          .map(_.flatten)

  def ack(receipt: Receipt): IO[QueueError, Unit] =
    sqs.delete(url, receipt.value).mapError(e => QueueError.Unavailable(e.message))
