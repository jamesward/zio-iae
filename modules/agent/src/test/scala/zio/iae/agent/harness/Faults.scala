package zio.iae.agent.harness

import zio.*
import zio.iae.EnvName
import zio.schema.{Schema, derived}

/** What can go wrong with a new version, injected by the harness while it's in canary. */
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

final case class ActiveFault(env: String, fault: Fault) derives Schema

/** The faults switched on per env; they apply to whichever version is in canary there. */
final case class Faults(active: Ref[Set[(EnvName, Fault)]]):
  def set(env: EnvName, fault: Fault, on: Boolean): UIO[Unit] = active.update(a => if on then a + (env -> fault) else a - (env -> fault))
  def in(env: EnvName): UIO[Set[Fault]]                       = active.get.map(_.collect { case (`env`, f) => f })
  def list: UIO[List[ActiveFault]]                             = active.get.map(_.toList.map((e, f) => ActiveFault(e.value, f)).sortBy(a => (a.env, a.fault.ordinal)))

object Faults:
  def make: UIO[Faults] = Ref.make(Set.empty[(EnvName, Fault)]).map(Faults(_))
