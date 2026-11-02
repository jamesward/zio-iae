package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*
import zio.json.*
import zio.json.ast.Json

/** Topic names: `<app>-<env>-<id>`. */
object TopicNaming:
  def physical(address: Address): String = s"${address.app.value}-${address.env.value}-${address.id.value}"
  def arn(context: AwsContext, address: Address): String =
    s"arn:aws:sns:${context.region.value}:${context.account.value}:${physical(address)}"

final case class ObservedTopic(arn: String, tags: Map[String, String], subscriptions: List[SnsSubscription])

final case class TopicProvider(sns: SnsClient, sqs: SqsClient, context: AwsContext) extends Provider[TopicSpec, ObservedTopic]:
  val kind: ResourceKind = ResourceKind.Topic

  def observe(address: Address): IO[ProviderError, Option[ObservedTopic]] =
    val arn = TopicNaming.arn(context, address)
    sns.exists(arn).flatMap: exists =>
      ZIO.when(exists)(sns.tags(arn).zipWith(sns.subscriptions(arn))(ObservedTopic(arn, _, _)))
    .mapError(failed(address, "observe"))

  def diff(address: Address, desired: TopicSpec, observed: Option[ObservedTopic]): Option[Change[TopicSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val wanted  = queueArns(address, desired)
        val present = current.subscriptions.filter(_.protocol == "sqs").map(_.endpoint).toSet
        val subs = (wanted -- present).toList.sorted.map(q => FieldChange("subscription", "(none)", q)) ++
          (present -- wanted).toList.sorted.map(q => FieldChange("subscription", q, "(none)"))
        val tags = ManagedTags.differences(current.tags, ManagedTags.identity(address, SpecHash.of(ResourceSpec.Topic(desired))))
        NonEmptyChunk.fromIterableOption(subs ++ tags).map(Change.Update(address, desired, _))

  def assess(change: Change[TopicSpec], observed: Option[ObservedTopic]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Medium, Set.empty)
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.High, Set.empty)
    case Change.Rollback(_, _, _) => Assessment(Risk.Low, Set.empty)

  def applyChange(change: Change[TopicSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    change match
      case Change.Create(_, spec) =>
        sns.createTopic(TopicNaming.physical(address), ManagedTags.converged(Map.empty, address, SpecHash.of(ResourceSpec.Topic(spec)), context))
          .mapError(failed(address, "create")) *> converge(address, spec, context)
      case Change.Update(_, spec, _) => converge(address, spec, context)
      case Change.Orphan(_, since) =>
        val arn = TopicNaming.arn(this.context, address)
        sns.tags(arn).flatMap(tags => sns.tag(arn, ManagedTags.orphaned(tags, since, context))).mapError(failed(address, "tag as orphaned"))
      case Change.Delete(_) => sns.deleteTopic(TopicNaming.arn(this.context, address)).mapError(failed(address, "delete"))
      case Change.Rollback(address, _, _) => ZIO.fail(ProviderError(address, "rollback", "only a service can be rolled back"))

  private def converge(address: Address, spec: TopicSpec, applying: ApplyContext): IO[ProviderError, Unit] =
    val arn = TopicNaming.arn(context, address)
    for
      current <- sns.subscriptions(arn).mapError(failed(address, "read subscriptions"))
      wanted   = spec.subscribers.map(s => queueAddress(address, s))
      _ <- ZIO.foreachDiscard(wanted): queue =>
             val queueArn = QueueNaming.arn(context, queueName(queue))
             allowDelivery(address, queue, arn) *>
               sns.subscribeQueue(arn, queueArn).mapError(failed(address, s"subscribe ${queue.id.value}"))
                 .unless(current.exists(s => s.protocol == "sqs" && s.endpoint == queueArn))
      wantedArns = wanted.map(q => QueueNaming.arn(context, queueName(q))).toSet
      _ <- ZIO.foreachDiscard(current.filter(s => s.protocol == "sqs" && !wantedArns.contains(s.endpoint))): s =>
             sns.unsubscribe(s.arn).mapError(failed(address, s"unsubscribe ${s.endpoint}"))
      tags   <- sns.tags(arn).mapError(failed(address, "read tags"))
      desired = ManagedTags.converged(tags, address, SpecHash.of(ResourceSpec.Topic(spec)), applying)
      _      <- sns.tag(arn, desired).mapError(failed(address, "tag"))
      _      <- sns.untag(arn, tags.keySet -- desired.keySet).mapError(failed(address, "untag"))
    yield ()

  /** Adds (or keeps) a statement in the queue's policy letting this topic deliver to it; other statements are kept. */
  private def allowDelivery(topic: Address, queue: Address, topicArn: String): IO[ProviderError, Unit] =
    val name = queueName(queue)
    for
      url <- sqs.queueUrl(name)
               .mapError(failed(topic, "find subscribed queue"))
               .someOrFail(ProviderError(topic, "subscribe", s"queue $name doesn't exist yet"))
               // Under auto-provisioning the queue's layer may still be creating it.
               .retry(Schedule.spaced(200.millis) && Schedule.recurs(50))
      attributes <- sqs.attributes(url).mapError(failed(topic, "read queue policy"))
      policy      = TopicProvider.withStatement(attributes.get("Policy"), TopicProvider.statement(topicArn, QueueNaming.arn(context, name)))
      _          <- sqs.setAttributes(url, Map("Policy" -> policy)).mapError(failed(topic, "set queue policy")).unless(attributes.get("Policy").contains(policy))
    yield ()

  private def queueAddress(topic: Address, subscriber: Subscriber): Address = Address(topic.app, topic.env, ResourceKind.Queue, subscriber.queue)

  private def queueName(queue: Address): String = s"${queue.app.value}-${queue.env.value}-${queue.id.value}"

  private def queueArns(address: Address, spec: TopicSpec): Set[String] =
    spec.subscribers.map(s => QueueNaming.arn(context, queueName(queueAddress(address, s)))).toSet

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

object TopicProvider:
  /** One statement per topic, identified by its Sid, so re-applying replaces it rather than adding duplicates. */
  def statement(topicArn: String, queueArn: String): Json =
    Json.Obj(
      "Sid"       -> Json.Str(s"iae-${topicArn.split(':').last}"),
      "Effect"    -> Json.Str("Allow"),
      "Principal" -> Json.Obj("Service" -> Json.Str("sns.amazonaws.com")),
      "Action"    -> Json.Str("sqs:SendMessage"),
      "Resource"  -> Json.Str(queueArn),
      "Condition" -> Json.Obj("ArnEquals" -> Json.Obj("aws:SourceArn" -> Json.Str(topicArn))),
    )

  /** Queue policies may hold statements written by others, in any shape, so this edits the JSON rather than a model. */
  def withStatement(existing: Option[String], statement: Json): String =
    def sidOf(json: Json): Option[String] = json.asObject.flatMap(_.get("Sid")).flatMap(_.asString)
    val sid = sidOf(statement)
    val others = existing.flatMap(_.fromJson[Json].toOption).flatMap(_.asObject).flatMap(_.get("Statement")).flatMap(_.asArray).getOrElse(Chunk.empty)
      .filterNot(s => sidOf(s) == sid)
    Json.Obj("Version" -> Json.Str("2012-10-17"), "Statement" -> Json.Arr(others :+ statement)).toJson

final case class TopicDriver(provider: TopicProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Topic

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Topic(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Topic(_)), assessment.risk, assessment.flags)
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a topic spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[TopicSpec]] = change match
      case Change.Create(address, ResourceSpec.Topic(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Topic(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                            => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                   => Right(Change.Delete(address))
      case other                                                    => Left(ProviderError(other.address, "apply", "expected a topic change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))

final case class SnsTopic[I <: Topic.Id[?]](id: I, sns: SnsClient, arn: String) extends TopicPublisher[I]:
  def publish[M](message: M)(using ev: I <:< Topic.Id[M]): IO[TopicError, Unit] =
    sns.publish(arn, Topic.encode(ev(id).schema, message)).mapError(e => TopicError.Unavailable(e.message))
