package zio.iae.aws

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.PriceListFake
import zio.test.*

/** What changes cost a month: a service's tasks, load balancer and alarms; a secret; nothing for a role. */
object PricingSpec extends ZIOSpecDefault:
  val app = AppName("photo-app")
  val env = EnvName("prod")

  def address(kind: ResourceKind, id: String) = Address(app, env, kind, LogicalId.from(id).toOption.get)

  def pricing: URIO[Client, AwsPricing] =
    ZIO.serviceWith[Client]: http =>
      val config = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), None)
      AwsPricing(Prices.fixed(PriceBook.usEast1), CloudWatchClient(AwsQueryClient(http, config)), Elbv2Client(AwsQueryClient(http, config)), BucketNaming(None))

  def spec = suite("Pricing")(
    test("a new service costs its tasks, its load balancer (one LCU) and its alarms on both colors"):
      for
        p    <- pricing
        spec  = ServiceSpec(8080, cpu = 512, memory = 1024, count = 2, canary = CanarySpec(latency = Some(LatencyGate(500))))
        cost <- p.estimate(PlannedChange(Change.Create(address(ResourceKind.Service, "web"), ResourceSpec.Service(spec)), Risk.Low, Set.empty))
        // 2 × (0.5 × 0.04048 + 1 × 0.004445) × 730 + (0.0225 + 0.008) × 730 + 2 gates × 2 colors × 0.10
        expected = 2 * (0.5 * 0.04048 + 0.004445) * 730 + 0.0305 * 730 + 0.4
      yield assertTrue(
        cost.exists(c => c.before == 0 && math.abs(c.after - expected) < 0.001),
        cost.exists(_.assumptions == List("the load balancer's traffic is one LCU")),
      )
    ,
    test("a secret costs its monthly price, exactly; a role nothing"):
      for
        p      <- pricing
        secret <- p.estimate(PlannedChange(Change.Create(address(ResourceKind.Secret, "key"), ResourceSpec.Secret(SecretSpec())), Risk.Low, Set.empty))
        role   <- p.estimate(PlannedChange(Change.Create(address(ResourceKind.Role, "runtime"), ResourceSpec.Role(RoleSpec(Nil, Nil))), Risk.Low, Set.empty))
      yield assertTrue(secret == Some(CostDelta(0, 0.40, exact = true)), role == Some(CostDelta(0, 0, exact = true)))
    ,
    test("a new queue's usage is assumed none, but its alarms cost"):
      for
        p    <- pricing
        cost <- p.estimate(PlannedChange(Change.Create(address(ResourceKind.Queue, "jobs"), ResourceSpec.Queue(QueueSpec(deadLetter = Some(DeadLetter(5))))), Risk.Low, Set.empty))
      yield assertTrue(cost.exists(c => math.abs(c.after - 0.20) < 1e-9 && !c.exact && c.assumptions.exists(_.contains("no usage yet"))))
    ,
    test("live prices come from the Price List API for the region, past free tiers; what it lacks keeps the bundled price"):
      ZIO.scoped:
        for
          fake   <- PriceListFake.make.orDie
          http   <- ZIO.service[Client]
          config  = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), Endpoint.parse(fake.url.encode).toOption)
          _      <- fake.add("AmazonECS", "eu-west-1", "EU-Fargate-vCPU-Hours:perCPU", 0.04456)
          _      <- fake.add("AmazonECS", "eu-west-1", "EU-Fargate-GB-Hours", 0.004865)
          _      <- fake.add("AmazonECS", "us-east-1", "USE1-Fargate-vCPU-Hours:perCPU", 0.04048)
          _      <- fake.add("AWSQueueService", "eu-west-1", "EU-Requests-Tier1", 0.0000004, free = Some(1e6))
          _      <- fake.add("AWSELB", "eu-west-1", "EU-LoadBalancerUsage", 0.0252, family = "Load Balancer-Application")
          _      <- fake.add("AWSELB", "eu-west-1", "EU-LoadBalancerUsage", 0.025, family = "Load Balancer")
          live    = LivePrices.make(PriceListClient(AwsJsonClient(http, config)), "eu-west-1")
          book   <- live.book
          again  <- live.book
          asked  <- fake.products.get.map(_.size)
        yield assertTrue(
          book.region == "eu-west-1",
          book("fargate.vcpu-hour") == 0.04456,
          book("fargate.gb-hour") == 0.004865,
          math.abs(book("sqs.million-requests") - 0.40) < 1e-9,
          // The application load balancer's, not the classic one's.
          book("alb.hour") == 0.0252,
          // Not in the fake: the bundled price.
          book("secretsmanager.secret-month") == 0.40,
          again == book,
          asked == 6,
        )
    ,
    test("when the Price List API can't be read, the bundled prices are used"):
      for
        http <- ZIO.service[Client]
        config = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), Endpoint.parse("http://localhost:1").toOption)
        book <- LivePrices.make(PriceListClient(AwsJsonClient(http, config)), "eu-west-1").book
      yield assertTrue(book == PriceBook.usEast1)
    ,
    test("capacity round-trips through its tag"):
      assertTrue(
        AwsPricing.Capacity.parse(AwsPricing.Capacity(256, 512, 3, 2).encode) == Some(AwsPricing.Capacity(256, 512, 3, 2)),
        AwsPricing.Capacity.parse("junk").isEmpty,
      ),
  ).provideShared(Client.default) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
