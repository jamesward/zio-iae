package zio.iae.engine

import zio.iae.*

enum EngineError derives CanEqual:
  case Provider(error: ProviderError)
  case NoDriver(kind: ResourceKind)

  /** The resource was last changed by a newer commit than the one this run targets: an older run must not undo it. */
  case Guarded(address: Address, resourceCommit: CommitSha, target: CommitSha)
  case CommitOrder(detail: String)
  case InvalidManifest(error: InfraError)

  def message: String = this match
    case Provider(error) => error.message
    case NoDriver(kind)  => s"no driver for resource kind $kind"
    case Guarded(address, resource, target) =>
      s"${address.show} was last changed by ${resource.short}, which is newer than ${target.short}; stopping"
    case CommitOrder(detail)    => s"could not order commits: $detail"
    case InvalidManifest(error) => error.message
