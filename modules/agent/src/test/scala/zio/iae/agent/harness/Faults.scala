package zio.iae.agent.harness

import zio.*
import zio.iae.EnvName
import zio.iae.aws.Color
import zio.schema.{Schema, derived}

/** What can go wrong with a version, injected by the harness. */
@zio.schema.annotation.simpleEnum
enum Fault derives Schema, CanEqual:
  /** It answers 5xx, as the load balancer counts them. */
  case Errors
  /** It answers slowly: two seconds on top of its real response time. */
  case Slow
  /** Its own failure metric climbs (`UploadFailures`, as the app would publish it). */
  case FailingMetric

  def describe: String = this match
    case Errors        => "answers 5xx"
    case Slow          => "answers slowly"
    case FailingMetric => "reports failing uploads"

/** Which version a fault hits: whichever is in canary (a bad deploy), or the version live when it was injected (an
  * incident), which keeps its fault after a rollback moves the traffic away from it.
  */
@zio.schema.annotation.simpleEnum
enum Target derives Schema, CanEqual:
  case Canary, Live

/** Where an active fault sits: `canary`, or a color (`blue`, `green`). */
final case class ActiveFault(env: String, fault: Fault, on: String) derives Schema

final case class Faults(active: Ref[Set[(EnvName, Fault, String)]]):
  /** `on` is `canary` or a color. */
  def set(env: EnvName, fault: Fault, on: String, enabled: Boolean): UIO[Unit] =
    active.update(a => if enabled then a + ((env, fault, on)) else a - ((env, fault, on)))

  /** The faults on a color: its own, plus the canary's while it isn't live. */
  def on(env: EnvName, color: Color, live: Boolean): UIO[Set[Fault]] =
    active.get.map(_.collect { case (`env`, f, where) if where == color.toString || (!live && where == Faults.Canary) => f })

  def list: UIO[List[ActiveFault]] =
    active.get.map(_.toList.map((e, f, w) => ActiveFault(e.value, f, w)).sortBy(a => (a.env, a.on, a.fault.ordinal)))

object Faults:
  val Canary = "canary"

  def make: UIO[Faults] = Ref.make(Set.empty[(EnvName, Fault, String)]).map(Faults(_))
