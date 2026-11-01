package zio.iae

import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum

/** What an app may do with a resource. Each capability maps to the IAM actions the app's role is granted. */
@simpleEnum
enum Capability derives Schema, CanEqual:
  case BucketRead, BucketWrite, QueueSend, QueueReceive, SecretRead

final case class Grant(id: LogicalId, kind: ResourceKind, capability: Capability) derives Schema, CanEqual
