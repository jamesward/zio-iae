package zio.iae.aws.client

import scala.xml.{Elem, Node}
import zio.*

final case class LoadBalancer(arn: String, name: String, dnsName: String, created: Option[java.time.Instant] = None)
final case class TargetGroup(arn: String, name: String, port: Int)
final case class Listener(arn: String, port: Int, weights: Map[String, Int])
final case class TargetHealth(id: String, port: Int, state: String)

/** Application load balancers: the load balancer, its target groups, and a listener whose weighted forward action
  * splits traffic between two target groups for a canary.
  */
final case class Elbv2Client(query: AwsQueryClient):
  import Elbv2Client.*

  def loadBalancer(name: String): IO[AwsError, Option[LoadBalancer]] =
    call("DescribeLoadBalancers", List("Names.member.1" -> name))
      .map(e => (e \\ "LoadBalancers" \ "member").headOption.map(loadBalancerOf))
      .catchSome { case e if e.errorCode.contains("LoadBalancerNotFound") => ZIO.none }

  def createLoadBalancer(name: String, subnets: List[String], tags: Map[String, String]): IO[AwsError, LoadBalancer] =
    val params = List("Name" -> name, "Type" -> "application", "Scheme" -> "internet-facing") ++
      members("Subnets", subnets) ++ tagParams(tags)
    call("CreateLoadBalancer", params).flatMap(e => single((e \\ "LoadBalancers" \ "member").headOption.map(loadBalancerOf), "CreateLoadBalancer"))

  def targetGroup(name: String): IO[AwsError, Option[TargetGroup]] =
    call("DescribeTargetGroups", List("Names.member.1" -> name))
      .map(e => (e \\ "TargetGroups" \ "member").headOption.map(targetGroupOf))
      .catchSome { case e if e.errorCode.contains("TargetGroupNotFound") => ZIO.none }

  def createTargetGroup(name: String, port: Int, vpc: String, healthPath: String, tags: Map[String, String]): IO[AwsError, TargetGroup] =
    val params = List(
      "Name" -> name, "Protocol" -> "HTTP", "Port" -> port.toString, "VpcId" -> vpc, "TargetType" -> "ip",
      "HealthCheckPath" -> healthPath, "HealthCheckIntervalSeconds" -> "10", "HealthyThresholdCount" -> "2",
    ) ++ tagParams(tags)
    call("CreateTargetGroup", params).flatMap(e => single((e \\ "TargetGroups" \ "member").headOption.map(targetGroupOf), "CreateTargetGroup"))

  def health(targetGroup: String): IO[AwsError, List[TargetHealth]] =
    call("DescribeTargetHealth", List("TargetGroupArn" -> targetGroup)).map: e =>
      (e \\ "TargetHealthDescriptions" \ "member").toList.map: m =>
        TargetHealth(text(m \ "Target" \ "Id"), text(m \ "Target" \ "Port").toIntOption.getOrElse(0), text(m \ "TargetHealth" \ "State"))

  def listeners(loadBalancer: String): IO[AwsError, List[Listener]] =
    call("DescribeListeners", List("LoadBalancerArn" -> loadBalancer)).map(e => (e \\ "Listeners" \ "member").toList.map(listenerOf))

  /** A listener forwarding to target groups with the given weights. */
  def createListener(loadBalancer: String, port: Int, weights: Map[String, Int]): IO[AwsError, Listener] =
    call("CreateListener", List("LoadBalancerArn" -> loadBalancer, "Protocol" -> "HTTP", "Port" -> port.toString) ++ forward("DefaultActions", weights))
      .flatMap(e => single((e \\ "Listeners" \ "member").headOption.map(listenerOf), "CreateListener"))

  def setWeights(listener: String, weights: Map[String, Int]): IO[AwsError, Unit] =
    call("ModifyListener", ("ListenerArn" -> listener) :: forward("DefaultActions", weights)).unit

  def tags(arn: String): IO[AwsError, Map[String, String]] =
    call("DescribeTags", List("ResourceArns.member.1" -> arn)).map: e =>
      (e \\ "Tags" \ "member").toList.map(m => text(m \ "Key") -> text(m \ "Value")).toMap

  def tag(arn: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call("AddTags", ("ResourceArns.member.1" -> arn) :: tagParams(tags)).unit.when(tags.nonEmpty).unit

  def deleteLoadBalancer(arn: String): IO[AwsError, Unit] = call("DeleteLoadBalancer", List("LoadBalancerArn" -> arn)).unit
  def deleteTargetGroup(arn: String): IO[AwsError, Unit]  = call("DeleteTargetGroup", List("TargetGroupArn" -> arn)).unit

  private def call(action: String, params: List[(String, String)]): IO[AwsError, Elem] = query.call(protocol, action, params)

object Elbv2Client:
  val layer: ZLayer[AwsQueryClient, Nothing, Elbv2Client] = ZLayer.fromFunction(Elbv2Client.apply)

  private val protocol = AwsQueryProtocol(AwsService("elasticloadbalancing", "elasticloadbalancing"), "2015-12-01")

  private def text(n: scala.xml.NodeSeq): String = n.headOption.map(_.text.trim).getOrElse("")

  private def single[A](a: Option[A], action: String): IO[AwsError, A] = ZIO.fromOption(a).orElseFail(AwsError.Decode("elasticloadbalancing", s"$action returned nothing"))

  private def loadBalancerOf(m: Node): LoadBalancer =
    LoadBalancer(text(m \ "LoadBalancerArn"), text(m \ "LoadBalancerName"), text(m \ "DNSName"),
      Option(text(m \ "CreatedTime")).filter(_.nonEmpty).flatMap(t => scala.util.Try(java.time.Instant.parse(t).nn).toOption))

  private def targetGroupOf(m: Node): TargetGroup = TargetGroup(text(m \ "TargetGroupArn"), text(m \ "TargetGroupName"), text(m \ "Port").toIntOption.getOrElse(0))

  private def listenerOf(m: Node): Listener =
    val actions = m \ "DefaultActions" \ "member"
    val weighted = (actions \ "ForwardConfig" \ "TargetGroups" \ "member").toList.map(t => text(t \ "TargetGroupArn") -> text(t \ "Weight").toIntOption.getOrElse(1))
    val single   = (actions \ "TargetGroupArn").toList.map(t => t.text.trim -> 100)
    Listener(text(m \ "ListenerArn"), text(m \ "Port").toIntOption.getOrElse(0), (if weighted.nonEmpty then weighted else single).toMap)

  private def members(name: String, values: List[String]): List[(String, String)] =
    values.zipWithIndex.map((v, i) => s"$name.member.${i + 1}" -> v)

  private def forward(prefix: String, weights: Map[String, Int]): List[(String, String)] =
    val p = s"$prefix.member.1"
    (s"$p.Type" -> "forward") :: weights.toList.sortBy(_._1).zipWithIndex.flatMap: (tw, i) =>
      List(s"$p.ForwardConfig.TargetGroups.member.${i + 1}.TargetGroupArn" -> tw._1, s"$p.ForwardConfig.TargetGroups.member.${i + 1}.Weight" -> tw._2.toString)

  private def tagParams(tags: Map[String, String]): List[(String, String)] =
    tags.toList.sorted.zipWithIndex.flatMap((kv, i) => List(s"Tags.member.${i + 1}.Key" -> kv._1, s"Tags.member.${i + 1}.Value" -> kv._2))
