package zio.iae

/** A failure talking to the cloud while observing or changing one resource. */
final case class ProviderError(address: Address, operation: String, detail: String) derives CanEqual:
  def message: String = s"$operation failed for ${address.show}: $detail"

enum InfraError derives CanEqual:
  /** Two declarations use the same logical id for different resources. */
  case ConflictingDeclarations(id: LogicalId, specs: List[ResourceSpec])

  /** A resource the app declares doesn't exist yet: the engine hasn't converged this env. */
  case NotProvisioned(address: Address)

  /** The resource exists but was provisioned from a different spec (or changed by hand). */
  case SpecMismatch(address: Address, expected: SpecHash, actual: Option[SpecHash])

  case InvalidConfig(detail: String)

  /** Something the program needs at startup (an HTTP client, credentials) couldn't be created. */
  case Unavailable(detail: String)

  case Provider(error: ProviderError)

  def message: String = this match
    case ConflictingDeclarations(id, specs) => s"logical id '${id.value}' is declared with different specs: $specs"
    case NotProvisioned(address)            => s"${address.show} is not provisioned"
    case SpecMismatch(address, expected, actual) =>
      s"${address.show} has spec hash ${actual.fold("none")(_.value)}, expected ${expected.value}"
    case InvalidConfig(detail) => s"invalid configuration: $detail"
    case Unavailable(detail)   => s"unavailable: $detail"
    case Provider(error)       => error.message
