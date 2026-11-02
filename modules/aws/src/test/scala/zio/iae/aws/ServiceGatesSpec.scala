package zio.iae.aws

import zio.iae.*
import zio.iae.aws.client.*
import zio.test.*

/** The alarms that gate a color's canary. */
object ServiceGatesSpec extends ZIOSpecDefault:
  val address = Address(AppName("photo-app"), EnvName("prod"), ResourceKind.Service, LogicalId("web"))
  val lb      = LoadBalancer("arn:aws:elasticloadbalancing:us-east-1:1:loadbalancer/app/iae-x/abc", "iae-x", "iae-x.elb")
  val group   = TargetGroup("arn:aws:elasticloadbalancing:us-east-1:1:targetgroup/iae-x-g/def", "iae-x-g", 80)

  def spec = suite("Service gates")(
    test("by default only the 5xx count gates, at the spec's threshold"):
      val gates = ServiceProvider.gates(address, Color.Green, lb, group, CanarySpec(maxErrors = 5))
      assertTrue(
        gates.map(_.alarm.name) == List("iae-photo-app-prod-web-green-5xx"),
        gates.head.alarm.threshold == 5.0,
        gates.head.alarm.dimensions == Map("LoadBalancer" -> "app/iae-x/abc", "TargetGroup" -> "targetgroup/iae-x-g/def"),
      )
    ,
    test("a latency gate is the response time's p99 in seconds, and app alarms read the app's metrics for that color"):
      val canary = CanarySpec(latency = Some(LatencyGate(750)), alarms = List(AppAlarm("uploads", "UploadFailures", 3), AppAlarm("orders", "Orders", 1, "Sum", below = true)))
      val gates  = ServiceProvider.gates(address, Color.Blue, lb, group, canary)
      val byName = gates.map(g => g.alarm.name.stripPrefix("iae-photo-app-prod-web-blue-") -> g.alarm).toMap
      assertTrue(
        gates.map(_.describe) == List("5xx", "latency (p99 over 750 ms)", "uploads", "orders"),
        byName("latency").statistic == "p99",
        byName("latency").threshold == 0.75,
        CloudWatchClient.percentile("p99"),
        !CloudWatchClient.percentile("Average"),
        byName("app-uploads").namespace == "iae/photo-app",
        byName("app-uploads").dimensions == Map("Service" -> "web", "Color" -> "blue"),
        byName("app-orders").comparison == "LessThanOrEqualToThreshold",
        ServiceProvider.metricsEnvironment(address, Color.Blue) ==
          Map("IAE_METRICS_NAMESPACE" -> "iae/photo-app", "IAE_SERVICE" -> "web", "IAE_COLOR" -> "blue"),
      ),
  )
