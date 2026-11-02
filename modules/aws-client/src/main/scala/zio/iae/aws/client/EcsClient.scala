package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}

/** One container of a task: its image, port and environment. */
final case class ContainerSpec(name: String, image: String, port: Int, environment: Map[String, String], cpu: Int, memory: Int, command: List[String] = Nil)

final case class EcsService(name: String, taskDefinition: String, desiredCount: Int, runningCount: Int, status: String)

/** ECS on Fargate: clusters, task definitions and services, each service attached to a load balancer target group. */
final case class EcsClient(json: AwsJsonClient):
  import EcsClient.*

  def clusterExists(name: String): IO[AwsError, Boolean] =
    call[ClustersRequest, Clusters]("DescribeClusters", ClustersRequest(List(name))).map(_.clusters.getOrElse(Nil).exists(c => c.clusterName == name && c.status.contains("ACTIVE")))

  def createCluster(name: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call[CreateCluster, AwsJsonClient.Empty]("CreateCluster", CreateCluster(name, tagList(tags))).unit

  /** Registers a revision of the family and returns its ARN. */
  def registerTaskDefinition(family: String, container: ContainerSpec, taskRole: Option[String], executionRole: Option[String]): IO[AwsError, String] =
    val definition = ContainerDefinition(
      container.name,
      container.image,
      true,
      List(PortMapping(container.port, "tcp")),
      container.environment.toList.sorted.map((k, v) => KeyValue(k, v)),
      Option.when(container.command.nonEmpty)(container.command),
    )
    call[RegisterTaskDefinition, Registered](
      "RegisterTaskDefinition",
      RegisterTaskDefinition(family, "awsvpc", List("FARGATE"), container.cpu.toString, container.memory.toString, taskRole, executionRole, List(definition)),
    ).map(_.taskDefinition.taskDefinitionArn)

  def service(cluster: String, name: String): IO[AwsError, Option[EcsService]] =
    call[ServicesRequest, Services]("DescribeServices", ServicesRequest(cluster, List(name)))
      .map(_.services.getOrElse(Nil).find(s => s.serviceName == name && !s.status.contains("INACTIVE")).map(serviceOf))
      .catchSome { case e if e.errorCode.exists(_.endsWith("ServiceNotFoundException")) => ZIO.none } // Floci answers 404

  def createService(cluster: String, name: String, taskDefinition: String, count: Int, subnets: List[String], targetGroup: String, container: String, port: Int, tags: Map[String, String])
      : IO[AwsError, Unit] =
    call[CreateService, AwsJsonClient.Empty](
      "CreateService",
      CreateService(
        cluster, name, taskDefinition, count, "FARGATE",
        NetworkConfiguration(AwsVpc(subnets, "ENABLED")),
        List(LoadBalancerRef(targetGroup, container, port)),
        tagList(tags),
      ),
    ).unit

  def updateService(cluster: String, name: String, taskDefinition: Option[String], count: Option[Int]): IO[AwsError, Unit] =
    call[UpdateService, AwsJsonClient.Empty]("UpdateService", UpdateService(cluster, name, taskDefinition, count)).unit

  def deleteService(cluster: String, name: String): IO[AwsError, Unit] =
    call[DeleteService, AwsJsonClient.Empty]("DeleteService", DeleteService(cluster, name, true)).unit

  /** The family's active revisions (ARNs), newest first. */
  def taskDefinitions(family: String): IO[AwsError, List[String]] =
    def page(token: Option[String]): IO[AwsError, List[String]] =
      call[ListTaskDefinitions, TaskDefinitionArns]("ListTaskDefinitions", ListTaskDefinitions(family, "ACTIVE", "DESC", token)).flatMap: r =>
        val arns = r.taskDefinitionArns.getOrElse(Nil)
        r.nextToken.filter(_.nonEmpty).fold(ZIO.succeed(arns))(t => page(Some(t)).map(arns ++ _))
    page(None).map(_.sortBy(arn => -EcsClient.revision(arn)))

  /** Takes a revision out of use: it can no longer start tasks or services. Already gone is fine. */
  def deregisterTaskDefinition(arn: String): IO[AwsError, Unit] =
    call[Deregister, AwsJsonClient.Empty]("DeregisterTaskDefinition", Deregister(arn)).unit
      .catchSome { case e if e.errorCode.exists(c => c.endsWith("ClientException") || c.endsWith("InvalidParameterException")) => ZIO.unit }

  private def call[Req: Schema, Res: Schema](operation: String, request: Req): IO[AwsError, Res] = json.call[Req, Res](protocol, operation, request)

object EcsClient:
  val layer: ZLayer[AwsJsonClient, Nothing, EcsClient] = ZLayer.fromFunction(EcsClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("ecs", "ecs"), "AmazonEC2ContainerServiceV20141113", "1.1")

  /** A task definition ARN's revision number (`...:task-definition/family:7` is 7). */
  def revision(arn: String): Int = arn.split(':').lastOption.flatMap(_.toIntOption).getOrElse(0)

  private def tagList(tags: Map[String, String]): List[Tag] = tags.toList.sorted.map((k, v) => Tag(k, v))

  private def serviceOf(s: ServiceJson): EcsService =
    EcsService(s.serviceName, s.taskDefinition.getOrElse(""), s.desiredCount.getOrElse(0), s.runningCount.getOrElse(0), s.status.getOrElse(""))

  final case class Tag(key: String, value: String) derives Schema
  final case class KeyValue(name: String, value: String) derives Schema
  final case class ClustersRequest(clusters: List[String]) derives Schema
  final case class ClusterJson(clusterName: String, status: Option[String]) derives Schema
  final case class Clusters(clusters: Option[List[ClusterJson]]) derives Schema
  final case class CreateCluster(clusterName: String, tags: List[Tag]) derives Schema
  final case class PortMapping(containerPort: Int, protocol: String) derives Schema
  final case class ContainerDefinition(
      name: String,
      image: String,
      essential: Boolean,
      portMappings: List[PortMapping],
      environment: List[KeyValue],
      command: Option[List[String]],
  ) derives Schema
  final case class RegisterTaskDefinition(
      family: String,
      networkMode: String,
      requiresCompatibilities: List[String],
      cpu: String,
      memory: String,
      taskRoleArn: Option[String],
      executionRoleArn: Option[String],
      containerDefinitions: List[ContainerDefinition],
  ) derives Schema
  final case class TaskDefinitionRef(taskDefinitionArn: String) derives Schema
  final case class Registered(taskDefinition: TaskDefinitionRef) derives Schema
  final case class ServicesRequest(cluster: String, services: List[String]) derives Schema
  final case class ServiceJson(serviceName: String, taskDefinition: Option[String], desiredCount: Option[Int], runningCount: Option[Int], status: Option[String]) derives Schema
  final case class Services(services: Option[List[ServiceJson]]) derives Schema
  final case class AwsVpc(subnets: List[String], assignPublicIp: String) derives Schema
  final case class NetworkConfiguration(awsvpcConfiguration: AwsVpc) derives Schema
  final case class LoadBalancerRef(targetGroupArn: String, containerName: String, containerPort: Int) derives Schema
  final case class CreateService(
      cluster: String,
      serviceName: String,
      taskDefinition: String,
      desiredCount: Int,
      launchType: String,
      networkConfiguration: NetworkConfiguration,
      loadBalancers: List[LoadBalancerRef],
      tags: List[Tag],
  ) derives Schema
  final case class UpdateService(cluster: String, service: String, taskDefinition: Option[String], desiredCount: Option[Int]) derives Schema
  final case class DeleteService(cluster: String, service: String, force: Boolean) derives Schema
  final case class ListTaskDefinitions(familyPrefix: String, status: String, sort: String, nextToken: Option[String]) derives Schema
  final case class TaskDefinitionArns(taskDefinitionArns: Option[List[String]], nextToken: Option[String]) derives Schema
  final case class Deregister(taskDefinition: String) derives Schema

/** An image in an ECR repository. */
final case class EcrImage(digest: String, tags: List[String], pushedAt: Option[java.time.Instant])

/** ECR repositories for the apps' images. */
final case class EcrClient(json: AwsJsonClient):
  import EcrClient.*

  /** The repository's URI, creating it if it's missing. */
  def ensureRepository(name: String, tags: Map[String, String]): IO[AwsError, String] =
    call[Names, Repositories]("DescribeRepositories", Names(List(name)))
      .map(_.repositories.getOrElse(Nil).headOption.map(_.repositoryUri))
      .catchSome { case e if e.errorCode.exists(_.endsWith("RepositoryNotFoundException")) => ZIO.none }
      .someOrElseZIO:
        call[Create, Created]("CreateRepository", Create(name, tags.toList.sorted.map((k, v) => EcsClient.Tag(k, v)))).map(_.repository.repositoryUri)

  /** Lets the accounts pull the repository's images (replacing whoever could before). */
  def setPullers(name: String, accounts: Set[String]): IO[AwsError, Unit] =
    val principals = accounts.toList.sorted.map(a => s"\"arn:aws:iam::$a:root\"").mkString(",")
    val policy =
      s"""{"Version":"2012-10-17","Statement":[{"Sid":"iae-pull","Effect":"Allow","Principal":{"AWS":[$principals]},""" +
        """"Action":["ecr:BatchGetImage","ecr:GetDownloadUrlForLayer","ecr:BatchCheckLayerAvailability"]}]}"""
    call[SetPolicy, AwsJsonClient.Empty]("SetRepositoryPolicy", SetPolicy(name, policy)).unit

  /** Every image in the repository: its digest, tags and when it was pushed. */
  def images(repository: String): IO[AwsError, List[EcrImage]] =
    def page(token: Option[String]): IO[AwsError, List[EcrImage]] =
      call[ListAll, Images]("DescribeImages", ListAll(repository, token)).flatMap: r =>
        val found = r.imageDetails.getOrElse(Nil).flatMap: d =>
          d.imageDigest.map(EcrImage(_, d.imageTags.getOrElse(Nil), d.imagePushedAt.map(s => java.time.Instant.ofEpochMilli((s * 1000).toLong).nn)))
        r.nextToken.filter(_.nonEmpty).fold(ZIO.succeed(found))(t => page(Some(t)).map(found ++ _))
    page(None).catchSome { case e if e.errorCode.exists(_.endsWith("RepositoryNotFoundException")) => ZIO.succeed(Nil) }

  /** Deletes images by digest, with all their tags. */
  def deleteImages(repository: String, digests: List[String]): IO[AwsError, Unit] =
    ZIO.foreachDiscard(digests.grouped(100).toList): batch =>
      call[DeleteBatch, AwsJsonClient.Empty]("BatchDeleteImage", DeleteBatch(repository, batch.map(Digest(_))))

  /** Whether an image with the tag is in the repository. */
  def hasImage(repository: String, tag: String): IO[AwsError, Boolean] =
    call[ImagesRequest, Images]("DescribeImages", ImagesRequest(repository, List(ImageId(tag))))
      .map(_.imageDetails.getOrElse(Nil).nonEmpty)
      .catchSome { case e if e.errorCode.exists(c => c.endsWith("ImageNotFoundException") || c.endsWith("RepositoryNotFoundException")) => ZIO.succeed(false) }

  private def call[Req: Schema, Res: Schema](operation: String, request: Req): IO[AwsError, Res] = json.call[Req, Res](protocol, operation, request)

object EcrClient:
  val layer: ZLayer[AwsJsonClient, Nothing, EcrClient] = ZLayer.fromFunction(EcrClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("ecr", "api.ecr"), "AmazonEC2ContainerRegistry_V20150921", "1.1")

  final case class Names(repositoryNames: List[String]) derives Schema
  final case class Repository(repositoryName: String, repositoryUri: String) derives Schema
  final case class Repositories(repositories: Option[List[Repository]]) derives Schema
  final case class Create(repositoryName: String, tags: List[EcsClient.Tag]) derives Schema
  final case class Created(repository: Repository) derives Schema
  final case class SetPolicy(repositoryName: String, policyText: String) derives Schema
  final case class ImageId(imageTag: String) derives Schema
  final case class ImagesRequest(repositoryName: String, imageIds: List[ImageId]) derives Schema
  final case class ImageDetail(imageTags: Option[List[String]], imageDigest: Option[String] = None, imagePushedAt: Option[Double] = None) derives Schema
  final case class Images(imageDetails: Option[List[ImageDetail]], nextToken: Option[String] = None) derives Schema
  final case class ListAll(repositoryName: String, nextToken: Option[String]) derives Schema
  final case class Digest(imageDigest: String) derives Schema
  final case class DeleteBatch(repositoryName: String, imageIds: List[Digest]) derives Schema
