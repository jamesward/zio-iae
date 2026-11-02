package zio.iae.aws

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** Who changed a resource, from CloudTrail's event history: the write calls on its physical name by anyone but the
  * pipeline's deployer (`deployer`, a role name, as in an assumed role's session ARN). CloudTrail records management
  * events for 90 days without a trail; the planner role needs `cloudtrail:LookupEvents`.
  */
final case class CloudTrailAttribution(cloudtrail: CloudTrailClient, buckets: BucketNaming, deployer: Option[String]) extends Attribution:
  def changes(address: Address, since: Instant): IO[ProviderError, List[ChangedBy]] =
    ZIO.foreach(CloudTrailAttribution.resourceName(address, buckets))(name =>
      cloudtrail.lookup(name, since).mapError(e => ProviderError(address, "who changed it", e.message))
    ).map(_.toList.flatten.filterNot(e => e.readOnly || deployer.exists(d => e.principal.exists(_.contains(s"/$d/")))).map: e =>
      ChangedBy(e.name, e.principal.orElse(e.username).getOrElse("unknown"), e.at))

object CloudTrailAttribution:
  /** The name CloudTrail records a resource's calls under. */
  def resourceName(address: Address, buckets: BucketNaming): Option[String] =
    address.kind match
      case ResourceKind.Bucket  => buckets.physical(address).toOption.map(_.value)
      case ResourceKind.Queue   => QueueNaming.physical(address).toOption
      case ResourceKind.Secret  => Some(SecretNaming.physical(address))
      case ResourceKind.Table   => Some(TableNaming.physical(address))
      case ResourceKind.Topic   => Some(TopicNaming.physical(address))
      case ResourceKind.Role    => RoleProvider.roleName(address.app, address.env).toOption.map(_.value)
      case ResourceKind.Service => Some(ServiceNaming.loadBalancer(address))
