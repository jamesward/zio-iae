package zio.iae.agent.harness

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import zio.*
import zio.schema.{Schema, derived}

final case class ClockState(now: Instant, speed: Double) derives Schema

/** The harness's clock for the agent and fake Slack: virtual time that runs `speed` times faster than real time and
  * can jump ahead, so expiries, reminders and periodic jobs happen in minutes. Sleeps are shortened by the speed;
  * a jump doesn't wake sleepers, so the harness signals the reconcilers after one. Work against real systems (AWS
  * signing, process timeouts) uses the live clock regardless.
  */
final class HarnessClock private (anchor: AtomicReference[HarnessClock.Anchor]) extends Clock:
  import HarnessClock.Anchor

  private def now(): Instant =
    val a = anchor.get.nn
    a.virtual.plusNanos(((java.lang.System.nanoTime - a.realNanos) * a.speed).toLong).nn

  def state: UIO[ClockState] = ZIO.succeed(ClockState(now(), anchor.get.nn.speed))

  def setSpeed(speed: Double): IO[String, Unit] =
    if speed < 1 || speed > 10000 then ZIO.fail(s"speed must be between 1 and 10000, not $speed")
    else ZIO.succeed(anchor.set(Anchor(now(), java.lang.System.nanoTime, speed)))

  def advance(by: Duration): IO[String, Unit] =
    if by.isNegative then ZIO.fail("the clock only moves forward")
    else ZIO.succeed(anchor.updateAndGet(a => a.copy(virtual = a.virtual.plus(by).nn)): Unit)

  def currentTime(unit: => TimeUnit)(implicit trace: Trace): UIO[Long] =
    ZIO.succeed(unit.convert(ChronoUnit.NANOS.between(Instant.EPOCH, now()), TimeUnit.NANOSECONDS))

  def currentTime(unit: => ChronoUnit)(implicit trace: Trace, d: DummyImplicit): UIO[Long] =
    ZIO.succeed(unit.between(Instant.EPOCH, now()))

  def currentDateTime(implicit trace: Trace): UIO[OffsetDateTime] = ZIO.succeed(OffsetDateTime.ofInstant(now(), ZoneOffset.UTC).nn)

  def instant(implicit trace: Trace): UIO[Instant] = ZIO.succeed(now())

  def javaClock(implicit trace: Trace): UIO[java.time.Clock] =
    ZIO.succeed:
      new java.time.Clock:
        def getZone: ZoneId                          = ZoneOffset.UTC
        override def withZone(zone: ZoneId): java.time.Clock = this
        override def instant: Instant               = now()

  def localDateTime(implicit trace: Trace): UIO[LocalDateTime] = ZIO.succeed(LocalDateTime.ofInstant(now(), ZoneOffset.UTC).nn)

  def nanoTime(implicit trace: Trace): UIO[Long] = currentTime(TimeUnit.NANOSECONDS)

  def scheduler(implicit trace: Trace): UIO[Scheduler] = Clock.ClockLive.scheduler

  def sleep(duration: => Duration)(implicit trace: Trace): UIO[Unit] =
    val d = duration
    Clock.ClockLive.sleep(Duration.fromNanos((d.toNanos / anchor.get.nn.speed).toLong))

object HarnessClock:
  private final case class Anchor(virtual: Instant, realNanos: Long, speed: Double)

  def make(speed: Double): UIO[HarnessClock] =
    ZIO.succeed(HarnessClock(AtomicReference(Anchor(Instant.now.nn, java.lang.System.nanoTime, speed))))
