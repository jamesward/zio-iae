package zio.iae.agent.harness

import zio.*
import zio.iae.EnvName
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

/** Which version a fault hits: the one in canary (a bad deploy), or the one with the traffic (an incident). */
@zio.schema.annotation.simpleEnum
enum Target derives Schema, CanEqual:
  case Canary, Live

final case class ActiveFault(env: String, fault: Fault, target: Target) derives Schema

/** The faults switched on per env and target. */
final case class Faults(active: Ref[Set[(EnvName, Fault, Target)]]):
  def set(env: EnvName, fault: Fault, target: Target, on: Boolean): UIO[Unit] =
    active.update(a => if on then a + ((env, fault, target)) else a - ((env, fault, target)))

  def in(env: EnvName, target: Target): UIO[Set[Fault]] = active.get.map(_.collect { case (`env`, f, `target`) => f })

  def list: UIO[List[ActiveFault]] =
    active.get.map(_.toList.map((e, f, t) => ActiveFault(e.value, f, t)).sortBy(a => (a.env, a.target.ordinal, a.fault.ordinal)))

object Faults:
  def make: UIO[Faults] = Ref.make(Set.empty[(EnvName, Fault, Target)]).map(Faults(_))
