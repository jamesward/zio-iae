package zio.iae

import zio.IO

/** The runtime binding from declared resources to a cloud. Provided at the edge of the program: production binds to
  * resources the engine provisioned; development and tests can provision on demand.
  */
trait Cloud:
  def app: AppName
  def env: EnvName
  def bucket[I](id: LogicalId, spec: BucketSpec): IO[InfraError, Bucket[I]]
  def queue[I <: MessageQueue.Id[?]](id: I): IO[InfraError, MessageQueue[I]]
  def secret[I](id: LogicalId, spec: SecretSpec): IO[InfraError, SecretReader[I]]
  def table[I <: Table.Id[?, ?]](id: I): IO[InfraError, Table[I]]
