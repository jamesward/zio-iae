package zio.iae.agent

import zio.*
import zio.test.*

/** The executor's per-env lanes: one request at a time per env, envs side by side, and no request queued twice. */
object LaneSpec extends ZIOSpecDefault:
  def request(env: String, plan: String) = ApplyRequest(env, "app", "c", "Expand", plan, None, "dev@example.com")

  def spec = suite("Lane")(
    test("a request already waiting isn't queued again, and requests run in order"):
      for
        gate    <- Promise.make[Nothing, Unit]
        handled <- Ref.make(List.empty[String])
        lane    <- Lane.make(r => gate.await *> handled.update(_ :+ r.plan))
        _       <- lane.offer(request("dev", "p1"))
        fiber   <- lane.run.fork
        // p1 is being handled (waiting on the gate); p2 waits, and its duplicate is dropped.
        _       <- lane.waiting.get.repeatUntil(_.isEmpty)
        _       <- lane.offer(request("dev", "p2")) *> lane.offer(request("dev", "p2"))
        _       <- gate.succeed(())
        done    <- handled.get.repeatUntil(_.size >= 2)
        _       <- ZIO.sleep(100.millis)
        all     <- handled.get
        _       <- fiber.interrupt
      yield assertTrue(done == List("p1", "p2"), all == List("p1", "p2")),
    test("a slow request in one lane doesn't hold up another"):
      for
        never   <- Promise.make[Nothing, Unit]
        handled <- Ref.make(List.empty[String])
        slow    <- Lane.make(_ => never.await)
        fast    <- Lane.make(r => handled.update(_ :+ r.env))
        fibers  <- ZIO.foreach(List(slow, fast))(_.run.fork)
        _       <- slow.offer(request("prod", "p1")) *> fast.offer(request("staging", "p2"))
        done    <- handled.get.repeatUntil(_.nonEmpty)
        _       <- ZIO.foreachDiscard(fibers)(_.interrupt)
      yield assertTrue(done == List("staging")),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
