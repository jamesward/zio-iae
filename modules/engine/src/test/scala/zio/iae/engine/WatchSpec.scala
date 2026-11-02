package zio.iae.engine

import zio.*
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.schema.{Schema, derived}
import zio.test.*

/** Queues and tables get default alarms when applied, and the engine watches them like a service's. */
object WatchSpec extends ZIOSpecDefault:
  val app = AppName("watch-app")

  final case class Order(id: String) derives Schema, CanEqual
  object Jobs   extends MessageQueue.Id[EngineSpec.Job](LogicalId("jobs"), QueueSpec(deadLetter = Some(DeadLetter(maxReceives = 5))))
  object Orders extends Table.Id[String, Order](LogicalId("orders"))

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  def spec = suite("Watching queues and tables on Floci")(
    test("a queue's age and dead letters and a table's throttles are watched; dead letters set off their alarm"):
      for
        clients  <- ZIO.environment[AwsEngine.Clients]
        engine    = AwsEngine.make(BucketNaming(None), clients, clients, AnyOrder)
        env      <- EngineSpec.uniqueEnv
        manifest <- ZIO.fromEither((MessageQueue.sender(Jobs) ++ Table.reader(Orders)).manifest(app)).mapError(e => EngineError.InvalidManifest(e))
        _        <- engine.converge(manifest, env, None)
        quiet    <- engine.alarms(app, env)
        cw        = clients.get[DeployClients].cloudwatch
        // As CloudWatch would after a 5-minute period of messages in the dead-letter queue (too long to wait for).
        _        <- cw.setState(s"iae-${app.value}-${env.value}-jobs-dead-letters", AlarmState.Alarm, "messages in the dead-letter queue")
        firing   <- engine.alarms(app, env)
      yield assertTrue(
        quiet.map(a => (a.address.kind, a.gate)).toSet ==
          Set(ResourceKind.Queue -> "age", ResourceKind.Queue -> "dead letters", ResourceKind.Table -> "read throttles", ResourceKind.Table -> "write throttles"),
        !quiet.exists(_.firing),
        firing.filter(_.firing).map(_.gate) == List("dead letters"),
      ),
  ).provideShared(FlociLayer.live, zio.http.Client.default, AwsEngine.clients) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
