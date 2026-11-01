package zio.iae

import zio.IO

/** The runtime binding from declared resources to a cloud. Provided at the edge of the program: production binds to
  * resources the engine provisioned; development and tests can provision on demand.
  */
trait Cloud:
  def app: AppName
  def env: EnvName
  def bucket[I](id: LogicalId, spec: BucketSpec): IO[InfraError, Bucket[I]]
