package zio.iae.testkit

import java.time.Instant
import zio.*
import zio.iae.*

/** An in-memory queue with SQS-like delivery: received messages are hidden for the visibility timeout and delivered
  * again unless acknowledged.
  */
private[testkit] final case class InMemoryQueue[I <: MessageQueue.Id[?]](id: I, state: Ref[InMemoryQueue.State]) extends MessageQueue[I]:
  import InMemoryQueue.*

  def send[M](message: M)(using ev: I <:< MessageQueue.Id[M]): IO[QueueError, Unit] =
    val body = MessageQueue.encode(ev(id).schema, message)
    state.update(s => s.copy(ready = s.ready :+ Stored(body, 0)))

  def receive[M](max: Int, wait: Duration)(using ev: I <:< MessageQueue.Id[M]): IO[QueueError, Chunk[Received[M]]] =
    for
      now      <- Clock.instant
      deadline  = now.plus(id.spec.visibilityTimeout).nn
      receipts <- ZIO.foreach(Chunk.range(0, max))(_ => Random.nextUUID.map(u => Receipt(u.toString)))
      taken <- state.modify: s =>
                 val expired  = s.inFlight.collect { case (_, (stored, until)) if !until.isAfter(now) => stored }
                 val ready    = s.ready ++ expired
                 val stillOut = s.inFlight.filter((_, v) => v._2.isAfter(now))
                 val (batch, rest) = ready.splitAt(max)
                 val delivered    = batch.zip(receipts).map((stored, receipt) => (receipt, stored.copy(receives = stored.receives + 1)))
                 (delivered, State(rest, stillOut ++ delivered.map((r, st) => r -> (st, deadline))))
      decoded <- ZIO.foreach(taken): (receipt, stored) =>
                   MessageQueue.decode(ev(id).schema, stored.body) match
                     case Right(message) => ZIO.some(Received(message, receipt, stored.receives))
                     case Left(error)    => ZIO.logWarning(s"undecodable message on ${id.logicalId.value}: $error").as(None)
    yield Chunk.fromIterable(decoded.flatten)

  def ack(receipt: Receipt): IO[QueueError, Unit] = state.update(s => s.copy(inFlight = s.inFlight - receipt))

private[testkit] object InMemoryQueue:
  final case class Stored(body: String, receives: Int)
  final case class State(ready: Vector[Stored], inFlight: Map[Receipt, (Stored, Instant)])

  val empty: State = State(Vector.empty, Map.empty)
