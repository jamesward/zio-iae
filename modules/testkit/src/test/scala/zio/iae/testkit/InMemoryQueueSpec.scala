package zio.iae.testkit

import zio.*
import zio.iae.*
import zio.schema.{Schema, derived}
import zio.test.*

object InMemoryQueueSpec extends ZIOSpecDefault:
  final case class Job(photo: String, width: Int) derives Schema, CanEqual
  object Jobs extends MessageQueue.Id[Job](LogicalId("jobs"), QueueSpec(visibilityTimeout = 30.seconds))

  val infra = MessageQueue.sendReceive(Jobs)

  def spec = suite("InMemoryQueue")(
    test("delivers typed messages, hides received ones, and redelivers what wasn't acknowledged"):
      val program =
        for
          jobs   <- ZIO.service[MessageQueue[Jobs.type]]
          _      <- jobs.send(Job("a.jpg", 100))
          _      <- jobs.send(Job("b.jpg", 200))
          first  <- jobs.receive(max = 1)
          second <- jobs.receive(max = 10)
          hidden <- jobs.receive(max = 10)
          _      <- ZIO.foreachDiscard(first)(r => jobs.ack(r.receipt))
          _      <- TestClock.adjust(31.seconds)
          again  <- jobs.receive(max = 10)
        yield assertTrue(
          first.map(_.message) == Chunk(Job("a.jpg", 100)),
          second.map(_.message) == Chunk(Job("b.jpg", 200)),
          hidden.isEmpty,
          again.map(r => (r.message, r.receiveCount)) == Chunk((Job("b.jpg", 200), 2)),
        )
      program.provide(InMemoryCloud.layer(AppName("photo-app"), EnvName("test")) >>> infra.layer)
    ,
    test("only the declared message type can be sent"):
      typeCheck("""
        val q: QueueSender[Jobs.type] = ???
        q.send("not a job")
      """).map(result => assertTrue(result.isLeft))
    ,
  )
