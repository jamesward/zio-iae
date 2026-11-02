package zio.iae.aws

import io.floci.testcontainers.FlociContainer
import zio.*
import zio.http.{Client, Request, URL}
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.test.*

/** The pieces a canary deploy is built from, on Floci: an ECS service behind blue/green target groups, an ALB
  * listener whose weights split the traffic (Floci serves it for real), and an alarm fed by metric data.
  */
object DeployClientsSpec extends ZIOSpecDefault:
  val subnets = List("subnet-default-us-east-1-a", "subnet-default-us-east-1-b")
  val vpc     = "vpc-default-us-east-1"

  def spec = suite("Deploy clients on Floci")(
    test("a service behind a weighted listener answers through the load balancer, and the weights move"):
      for
        floci   <- ZIO.service[FlociContainer]
        aws     <- ZIO.service[AwsConfig]
        http    <- ZIO.service[Client]
        suffix  <- Random.nextIntBounded(100000)
        json     = AwsJsonClient(http, aws)
        ecs      = EcsClient(json)
        elb      = Elbv2Client(AwsQueryClient(http, aws))
        cluster  = s"iae-test-$suffix"
        _       <- ecs.createCluster(cluster, Map("iae:managed" -> "true"))
        exists  <- ecs.clusterExists(cluster)
        task    <- ecs.registerTaskDefinition(s"web-$suffix", ContainerSpec("web", "nginx:alpine", 80, Map("GREETING" -> "hi"), 256, 512), None, None)
        lb      <- elb.createLoadBalancer(s"lb-$suffix", subnets, Map("iae:managed" -> "true"))
        blue    <- elb.createTargetGroup(s"blue-$suffix", 80, vpc, "/", Map.empty)
        green   <- elb.createTargetGroup(s"green-$suffix", 80, vpc, "/", Map.empty)
        port     = 18000 + suffix % 1000
        listener <- elb.createListener(lb.arn, port, Map(blue.arn -> 100, green.arn -> 0))
        _       <- ecs.createService(cluster, "web-blue", task, 1, subnets, blue.arn, "web", 80, Map.empty)
        targets <- elb.health(blue.arn).repeat(Schedule.spaced(1.second) && Schedule.recurUntil[List[TargetHealth]](_.nonEmpty) && Schedule.recurs(60)).map(_._2)
        ip       = floci.getContainerInfo.nn.getNetworkSettings.nn.getNetworks.nn.values.nn.iterator.nn.next.nn.getIpAddress.nn
        status  <- http.batched(Request.get(URL.decode(s"http://$ip:$port/").toOption.get)).map(_.status.code)
                     .retry(Schedule.spaced(1.second) && Schedule.recurs(30))
        _       <- elb.setWeights(listener.arn, Map(blue.arn -> 10, green.arn -> 90))
        moved   <- elb.listeners(lb.arn).map(_.find(_.arn == listener.arn).map(_.weights))
        service <- ecs.service(cluster, "web-blue")
        found   <- elb.targetGroup(s"green-$suffix")
        missing <- elb.targetGroup(s"nope-$suffix")
      yield assertTrue(
        exists,
        targets.nonEmpty,
        status == 200,
        moved.contains(Map(blue.arn -> 10, green.arn -> 90)),
        service.exists(_.desiredCount == 1),
        found.map(_.arn).contains(green.arn),
        missing.isEmpty,
      ),
    test("an alarm goes off from metric data, and its state can be set"):
      for
        aws    <- ZIO.service[AwsConfig]
        http   <- ZIO.service[Client]
        suffix <- Random.nextIntBounded(100000)
        cw      = CloudWatchClient(AwsQueryClient(http, aws))
        name    = s"errors-$suffix"
        _      <- cw.putAlarm(MetricAlarm(name, "iae/test", "Errors", Map("Service" -> s"web-$suffix"), "Sum", 10, 1, 1, "GreaterThanOrEqualToThreshold"))
        before <- cw.states(List(name))
        // Published on every check, as errors keep coming: a single point can fall out of the period before Floci
        // gets to evaluate it when it's busy.
        publish = cw.putMetric("iae/test", "Errors", 5, Map("Service" -> s"web-$suffix"))
        after  <- (publish *> cw.states(List(name)))
                    .repeat(Schedule.spaced(1.second) && Schedule.recurUntil[Map[String, AlarmState]](_.get(name).contains(AlarmState.Alarm)) && Schedule.recurs(90))
                    .map(_._2)
        _      <- cw.setState(name, AlarmState.Ok, "test")
        reset  <- cw.states(List(name))
      yield assertTrue(!before.get(name).contains(AlarmState.Alarm), after.get(name).contains(AlarmState.Alarm), reset.get(name).contains(AlarmState.Ok)),
    test("an ECR repository is created once, and images are looked up by tag"):
      for
        aws    <- ZIO.service[AwsConfig]
        http   <- ZIO.service[Client]
        suffix <- Random.nextIntBounded(100000)
        ecr     = EcrClient(AwsJsonClient(http, aws))
        uri    <- ecr.ensureRepository(s"photo-$suffix", Map("iae:managed" -> "true"))
        again  <- ecr.ensureRepository(s"photo-$suffix", Map.empty)
        image  <- ecr.hasImage(s"photo-$suffix", "abc")
      yield assertTrue(uri.endsWith(s"photo-$suffix"), again == uri, !image),
  ).provideShared(FlociLayer.container, FlociLayer.awsConfig, Client.default) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(5.minutes)
