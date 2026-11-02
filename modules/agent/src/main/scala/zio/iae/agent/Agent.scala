package zio.iae.agent

import zio.*
import zio.iae.aws.client.{QueueUrl, SqsClient}
import zio.iae.engine.InboxEvent

/** One reconcile loop per env. Signals coalesce: however many arrive while a reconcile runs, one more runs after it,
  * against whatever the branch head is then. A converged env signals the next one.
  */
final case class Reconcilers(pipeline: Pipeline, envs: List[EnvRuntime], signals: Chunk[Queue[Unit]]):
  def signalAll: UIO[Unit] = ZIO.foreachDiscard(signals)(_.offer(()))

  def run: UIO[Nothing] =
    ZIO.foreachParDiscard(envs.indices)(i => loop(i).forever) *> ZIO.never

  private def loop(index: Int): UIO[Unit] =
    signals(index).take *>
      pipeline
        .reconcile(envs, index)
        .flatMap:
          case Outcome.Converged(_) => ZIO.foreachDiscard(signals.lift(index + 1))(_.offer(()))
          case _                    => ZIO.unit
        .catchAllCause(c => ZIO.logErrorCause(s"reconciling ${envs(index).env.value} failed", c))

object Reconcilers:
  def make(pipeline: Pipeline, envs: List[EnvRuntime]): UIO[Reconcilers] =
    ZIO.foreach(Chunk.fromIterable(envs))(_ => Queue.sliding[Unit](1)).map(Reconcilers(pipeline, envs, _))

/** Reads the inbox. Every event is only a reason to reconcile: the pipeline re-reads git, the cloud and Slack. */
final case class InboxConsumer(sqs: SqsClient, url: QueueUrl, branch: String, reconcilers: Reconcilers, conversations: Option[Conversations] = None):
  def run: UIO[Nothing] = poll.forever

  private def poll: UIO[Unit] =
    sqs
      .receive(url, 10, 20)
      .flatMap: messages =>
        ZIO.foreachDiscard(messages): message =>
          handle(message.body) *> sqs.delete(url, message.receipt)
      .catchAllCause(c => ZIO.logErrorCause("reading the inbox failed", c).delay(5.seconds))

  def handle(body: String): UIO[Unit] =
    InboxEvent.decode(body) match
      case Left(error) => ZIO.logWarning(s"ignoring an unreadable inbox message: $error")
      case Right(InboxEvent.Pushed(_, _, b, _)) if b != branch => ZIO.unit
      case Right(event @ (_: InboxEvent.SlackReply | _: InboxEvent.SlackMention)) =>
        ZIO.logDebug(s"inbox: $event") *> reconcilers.signalAll *> ZIO.foreachDiscard(conversations)(_.offer(event))
      case Right(event) => ZIO.logDebug(s"inbox: $event") *> reconcilers.signalAll

/** The always-on agent: reconcile everything at start (that is all a restart needs), then react to the inbox and a
  * periodic tick, which also covers events that were dropped. The monitor watches the live services meanwhile.
  */
final case class Agent(reconcilers: Reconcilers, inbox: InboxConsumer, every: Duration, monitor: Option[Monitor] = None):
  def run: UIO[Nothing] =
    reconcilers.signalAll *>
      (reconcilers.run <&> inbox.run <&> ZIO.foreachDiscard(inbox.conversations)(_.run) <&> ZIO.foreachDiscard(monitor)(_.run) <&>
        reconcilers.signalAll.repeat(Schedule.spaced(every)).forever)
        .as(())
        .forever
