package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.aws.*
import zio.iae.aws.client.*

/** The engine wired to AWS: one driver per resource kind, discovery through the tagging API plus IAM. */
object AwsEngine:
  def drivers(
      s3: S3Client,
      sqs: SqsClient,
      secrets: SecretsClient,
      dynamo: DynamoClient,
      sns: SnsClient,
      iam: IamClient,
      deploys: DeployClients,
      naming: BucketNaming,
      context: AwsContext,
  ): Drivers =
    Drivers(
      List(
        BucketDriver(BucketProvider(s3, naming)),
        QueueDriver(QueueProvider(sqs, Some(deploys.cloudwatch))),
        SecretDriver(SecretProvider(secrets)),
        TableDriver(TableProvider(dynamo, Some(deploys.cloudwatch))),
        TopicDriver(TopicProvider(sns, sqs, context)),
        RoleDriver(RoleProvider(iam, naming, context)),
        ServiceDriver(ServiceProvider(deploys.ecs, deploys.elb, deploys.cloudwatch, deploys.ec2.defaultNetwork.mapError(_.message).either, context)),
      )
    )

  def discovery(tagging: TaggingClient, iam: IamClient): CompositeDiscovery =
    CompositeDiscovery(List(TaggingDiscovery(tagging), RoleDiscovery(iam)))

  type Clients = AwsCloud.Clients & TaggingClient & IamClient & DeployClients

  /** Every client the engine needs, from an `AwsConfig` and an HTTP client. */
  val clients: ZLayer[AwsConfig & Client, AwsError, Clients] =
    AwsCloud.clients ++ ((AwsJsonClient.layer >>> TaggingClient.layer) ++ (AwsQueryClient.layer >>> IamClient.layer)) ++
      ((AwsJsonClient.layer ++ AwsQueryClient.layer) >>> DeployClients.layer)

  /** The engine from one set of clients for both planning and applying (a single account, the CLI, tests). */
  def layer(naming: BucketNaming): ZLayer[Clients & CommitOrder, Nothing, Engine] =
    ZLayer:
      for
        clients <- ZIO.environment[Clients]
        commits <- ZIO.service[CommitOrder]
      yield make(naming, clients, clients, commits)

  /** Plans with the planner's clients and applies with the deployer's, e.g. from an env account's read-only role and
    * its deploy role.
    */
  def make(naming: BucketNaming, planner: ZEnvironment[Clients], deployer: ZEnvironment[Clients], commits: CommitOrder): Engine =
    def driversOf(c: ZEnvironment[Clients]) =
      drivers(c.get[S3Client], c.get[SqsClient], c.get[SecretsClient], c.get[DynamoClient], c.get[SnsClient], c.get[IamClient], c.get[DeployClients], naming, c.get[AwsContext])
    val found = discovery(planner.get[TaggingClient], planner.get[IamClient])
    val deploy = planner.get[DeployClients]
    val watch = ServiceWatch(
      ServiceProvider(deploy.ecs, deploy.elb, deploy.cloudwatch, deploy.ec2.defaultNetwork.mapError(_.message).either, planner.get[AwsContext]),
      found,
    )
    // The pipeline's own calls come from the deployer role bootstrap creates in each env account.
    val attribution = CloudTrailAttribution(planner.get[DeployClients].cloudtrail, naming, Some("iae-deployer"))
    val prices  = LivePrices.make(deploy.prices, planner.get[AwsContext].region.value)
    val pricing = AwsPricing(prices, deploy.cloudwatch, deploy.elb, naming)
    Engine(Planner(driversOf(planner), found), zio.iae.engine.Executor(driversOf(deployer), commits), watch, attribution, pricing, IamAccess(planner.get[IamClient]),
      AwsRecommendations(ComputeOptimizerRecommendations(deploy.optimizer, watch.provider, found, prices),
        StorageAdvice(planner.get[S3Client], deploy.cloudwatch, found, naming, prices)), AwsActivity(deploy.cloudwatch, deploy.elb, planner.get[SqsClient], planner.get[DynamoClient], naming),
      CostExplorerBills(deploy.costs),
      AwsFindings(found, naming, ConfigChecks(planner.get[S3Client], planner.get[SqsClient], deploy.elb, naming),
        SecurityHubClient(deploy.rest), GuardDutyClient(deploy.rest), AccessAnalyzerClient(deploy.rest)))

  /** The clients for one account's config. */
  def clientsFor(config: AwsConfig): ZIO[Client & Scope, AwsError, ZEnvironment[Clients]] =
    (ZLayer.succeed(config) ++ ZLayer.environment[Client] >>> clients).build

/** The clients a service's deploys use: ECS, the load balancer, its alarms, and the default network. */
final case class DeployClients(
    ecs: EcsClient,
    elb: Elbv2Client,
    cloudwatch: CloudWatchClient,
    ec2: Ec2Client,
    cloudtrail: CloudTrailClient,
    optimizer: ComputeOptimizerClient,
    costs: CostExplorerClient,
    prices: PriceListClient,
    rest: AwsRestJsonClient,
)

object DeployClients:
  val layer: ZLayer[AwsJsonClient & AwsQueryClient, Nothing, DeployClients] =
    ZLayer.fromFunction((json: AwsJsonClient, query: AwsQueryClient) =>
      DeployClients(EcsClient(json), Elbv2Client(query), CloudWatchClient(query), Ec2Client(query), CloudTrailClient(json), ComputeOptimizerClient(json), CostExplorerClient(json), PriceListClient(json), AwsRestJsonClient(json.http, json.config)))
