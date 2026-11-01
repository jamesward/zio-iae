package zio.iae

import zio.*
import zio.schema.{Schema, derived}

/** After `maxReceives` failed deliveries a message moves to the queue's dead-letter queue. */
final case class DeadLetter(maxReceives: Int) derives Schema, CanEqual

final case class QueueSpec(
    visibilityTimeout: Duration = 30.seconds,
    retention: Duration = 4.days,
    deadLetter: Option[DeadLetter] = None,
) derives Schema, CanEqual

opaque type Receipt = String

object Receipt:
  def apply(value: String): Receipt = value

  extension (receipt: Receipt) def value: String = receipt

  given CanEqual[Receipt, Receipt] = CanEqual.derived

/** A received message: acknowledge it with its receipt once handled, or it is delivered again. */
final case class Received[M](message: M, receipt: Receipt, receiveCount: Int)

enum QueueError derives CanEqual:
  case Unavailable(detail: String)

  def message: String = this match
    case Unavailable(detail) => s"queue unavailable: $detail"

/** Sends messages of the queue's declared type: `Jobs` declared as `MessageQueue.Id[Job]` only accepts `Job`s. */
trait QueueSender[I]:
  def send[M](message: M)(using I <:< MessageQueue.Id[M]): IO[QueueError, Unit]

trait QueueReceiver[I]:
  /** Waits up to `wait` for up to `max` messages. Messages that can't be decoded are left to go to the dead-letter
    * queue.
    */
  def receive[M](max: Int = 10, wait: Duration = 20.seconds)(using I <:< MessageQueue.Id[M]): IO[QueueError, Chunk[Received[M]]]

  def ack(receipt: Receipt): IO[QueueError, Unit]

trait MessageQueue[I] extends QueueSender[I], QueueReceiver[I]

object MessageQueue:
  /** Messages travel as JSON derived from the queue's message schema. */
  def encode[M](schema: Schema[M], message: M): String =
    zio.schema.codec.JsonCodec.jsonEncoder(schema).encodeJson(message, None).toString

  def decode[M](schema: Schema[M], body: String): Either[String, M] =
    zio.schema.codec.JsonCodec.jsonDecoder(schema).decodeJson(body)

  /** Declares a queue of `M`s. Extend it with an object:
    * {{{
    * object Jobs extends MessageQueue.Id[Job](LogicalId("jobs"), QueueSpec(deadLetter = Some(DeadLetter(maxReceives = 5))))
    * }}}
    */
  abstract class Id[M](val logicalId: LogicalId, val spec: QueueSpec = QueueSpec())(using val schema: Schema[M])

  def sender[I <: Id[?] & Singleton](id: I)(using Tag[QueueSender[I]]): Infra[QueueSender[I]] =
    declare(id, Chunk(Capability.QueueSend))

  def receiver[I <: Id[?] & Singleton](id: I)(using Tag[QueueReceiver[I]]): Infra[QueueReceiver[I]] =
    declare(id, Chunk(Capability.QueueReceive))

  def sendReceive[I <: Id[?] & Singleton](id: I)(using Tag[MessageQueue[I]]): Infra[MessageQueue[I]] =
    declare(id, Chunk(Capability.QueueSend, Capability.QueueReceive))

  /** Declares a queue the app doesn't use itself, for example one a topic delivers to for another service. */
  def wired[I <: Id[?] & Singleton](id: I): Infra[Any] =
    Infra(Chunk(ResourceDecl(id.logicalId, ResourceSpec.Queue(id.spec))), Chunk.empty, ZLayer.empty)

  private def declare[I <: Id[?] & Singleton, R >: MessageQueue[I]: Tag](id: I, capabilities: Chunk[Capability]): Infra[R] =
    Infra(
      resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Queue(id.spec))),
      grants = capabilities.map(Grant(id.logicalId, ResourceKind.Queue, _)),
      layer = ZLayer.fromZIO[Cloud, InfraError, R](ZIO.serviceWithZIO[Cloud](_.queue[I](id))),
    )
