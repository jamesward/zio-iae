package zio.iae.agent

import zio.*
import zio.iae.agent.harness.HarnessClock
import zio.test.*

object HarnessClockSpec extends ZIOSpecDefault:
  def spec = suite("HarnessClock")(
    test("time runs at the speed factor and sleeps are shortened by it"):
      for
        clock   <- HarnessClock.make(3600)
        before  <- clock.instant
        started <- Clock.ClockLive.nanoTime
        _       <- clock.sleep(1.hour)
        real    <- Clock.ClockLive.nanoTime.map(n => Duration.fromNanos(n - started))
        after   <- clock.instant
      yield assertTrue(real < 3.seconds, java.time.Duration.between(before, after).nn.toMinutes >= 59),
    test("advance jumps ahead, and a speed change keeps the current time"):
      for
        clock  <- HarnessClock.make(1)
        before <- clock.instant
        _      <- clock.advance(1.day)
        _      <- clock.setSpeed(100)
        after  <- clock.instant
        state  <- clock.state
        back   <- clock.advance(Duration.fromSeconds(-1)).flip
        slow   <- clock.setSpeed(0).flip
      yield assertTrue(
        java.time.Duration.between(before, after).nn.toHours == 24,
        state.speed == 100.0,
        back.contains("forward"),
        slow.contains("between"),
      ),
    test("ZIO code under the clock sees its time"):
      for
        clock <- HarnessClock.make(1)
        _     <- clock.advance(365.days)
        seen  <- ZIO.withClock(clock)(Clock.instant)
        real  <- Clock.ClockLive.instant
      yield assertTrue(seen.isAfter(real.plus(java.time.Duration.ofDays(364)))),
  )
