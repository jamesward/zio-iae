package zio.iae.aws.client

import zio.*

/** Where services run: a VPC and its subnets. */
final case class Network(vpc: String, subnets: List[String])

/** The default VPC's network, for services that don't name one. */
final case class Ec2Client(query: AwsQueryClient):
  def defaultNetwork: IO[AwsError, Network] =
    for
      vpcs    <- query.call(Ec2Client.protocol, "DescribeVpcs", List("Filter.1.Name" -> "isDefault", "Filter.1.Value.1" -> "true"))
      vpc     <- ZIO.fromOption((vpcs \\ "vpcId").headOption.map(_.text.trim)).orElseFail(AwsError.Decode("ec2", "no default VPC"))
      subnets <- query.call(Ec2Client.protocol, "DescribeSubnets", List("Filter.1.Name" -> "vpc-id", "Filter.1.Value.1" -> vpc))
    yield Network(vpc, (subnets \\ "subnetId").map(_.text.trim).toList.distinct.sorted)

object Ec2Client:
  private val protocol = AwsQueryProtocol(AwsService("ec2", "ec2"), "2016-11-15")
