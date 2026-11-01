package zio.iae

import zio.*
import zio.schema.{Schema, derived}

/** How a new version takes traffic: the share (in percent) the new version gets at each step, how long each step
  * bakes while its alarms are watched, and the alarms' period (60 seconds for the load balancer's metrics). The last
  * step should be 100.
  */
final case class CanarySpec(steps: List[Int] = List(10, 50, 100), bakeSeconds: Int = 300, alarmPeriodSeconds: Int = 60) derives Schema, CanEqual

/** The app running as a web service: containers of its image behind a load balancer, deployed by canary.
  * `environment` and `command` are passed to the container as they are. `image` is filled in by the agent after a
  * build (from the app's container in `.iae/config.json`), so a new commit's image is a change to the service like
  * any other.
  */
final case class ServiceSpec(
    port: Int,
    healthPath: String = "/health",
    cpu: Int = 256,
    memory: Int = 512,
    count: Int = 1,
    listenerPort: Int = 80,
    canary: CanarySpec = CanarySpec(),
    environment: Map[String, String] = Map.empty,
    command: List[String] = Nil,
    image: Option[String] = None,
) derives Schema, CanEqual

object Service:
  /** {{{ object Web extends Service.Id(LogicalId("web"), ServiceSpec(port = 8080)) }}} */
  abstract class Id(val logicalId: LogicalId, val spec: ServiceSpec)

  /** Declares that the app runs as this service. It gives the program nothing to call: the program is the service. */
  def run[I <: Id & Singleton](id: I): Infra[Any] =
    Infra(resources = Chunk(ResourceDecl(id.logicalId, ResourceSpec.Service(id.spec))), grants = Chunk.empty, layer = ZLayer.empty)
