package zio.iae

import zio.*
import zio.schema.{Schema, derived}

final case class Subscriber(queue: LogicalId) derives Schema, CanEqual

/** Who receives a topic's messages: queues of the same app, each getting every message as-is (raw delivery). */
final case class TopicSpec(subscribers: List[Subscriber] = Nil) derives Schema, CanEqual

enum TopicError derives CanEqual:
  case Unavailable(detail: String)

  def message: String = this match
    case Unavailable(detail) => s"topic unavailable: $detail"

trait TopicPublisher[I]:
  def publish[M](message: M)(using I <:< Topic.Id[M]): IO[TopicError, Unit]

object Topic:
  /** A subscription of a queue whose message type matches the topic's. */
  final case class Subscription[M] private[Topic] (queue: LogicalId)

  def to[M](queue: MessageQueue.Id[M]): Subscription[M] = Subscription(queue.logicalId)

  /** Declares a topic of `M`s and the queues that receive them; a queue of another message type doesn't compile:
    * {{{
    * object PhotoEvents extends Topic.Id[PhotoEvent](LogicalId("photo-events"), List(Topic.to(Thumbnailer), Topic.to(Indexer)))
    * }}}
    */
  abstract class Id[M](val logicalId: LogicalId, subscriptions: List[Subscription[M]] = Nil)(using val schema: Schema[M]):
    val spec: TopicSpec = TopicSpec(subscriptions.map(s => Subscriber(s.queue)).distinct.sortBy(_.queue))

  def publisher[I <: Id[?] & Singleton](id: I)(using Tag[TopicPublisher[I]]): Infra[TopicPublisher[I]] =
    Infra(
      resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Topic(id.spec))),
      grants = Chunk(Grant(id.logicalId, ResourceKind.Topic, Capability.TopicPublish)),
      layer = ZLayer.fromZIO[Cloud, InfraError, TopicPublisher[I]](ZIO.serviceWithZIO[Cloud](_.topic[I](id))),
    )

  def encode[M](schema: Schema[M], message: M): String = MessageQueue.encode(schema, message)
