package zio.iae.aws

import java.time.LocalDate
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.CostExplorerFake
import zio.json.ast.Json
import zio.test.*

/** The bill from Cost Explorer: an app's cost in an env by its tags, by service, and the anomalies in its account. */
object BillsSpec extends ZIOSpecDefault:
  val app = AppName("photo-app")
  val env = EnvName("prod")

  def spec = suite("Bills")(
    test("an app's cost is read by its iae:app and iae:env tags, by service; anomalies come with their causes"):
      ZIO.scoped:
        for
          fake   <- CostExplorerFake.make.orDie
          http   <- ZIO.service[Client]
          config  = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), Endpoint.parse(fake.url.encode).toOption)
          bills   = CostExplorerBills(CostExplorerClient(AwsJsonClient(http, config)))
          mine    = Map("iae:app" -> "photo-app", "iae:env" -> "prod")
          _      <- fake.cost(mine, "Amazon Elastic Container Service", 30.25)
          _      <- fake.cost(mine, "Amazon Simple Storage Service", 1.5)
          _      <- fake.cost(Map("iae:app" -> "photo-app", "iae:env" -> "staging"), "Amazon Elastic Container Service", 9)
          _      <- fake.anomaly(CostExplorerFake.Anomaly("arn:aws:ce::1:anomalymonitor/m/a1", "2026-10-30", "AmazonEC2", 42.5, "Amazon Elastic Compute Cloud - Compute"))
          from    = LocalDate.parse("2026-10-03").nn
          spend  <- bills.spend(app, env, from, from.plusDays(30).nn)
          asked  <- fake.requests.get.map(_.head)
          found  <- bills.anomalies(env, from)
        yield assertTrue(
          spend.map(_.total).exists(t => math.abs(t - 31.75) < 0.001),
          spend.map(_.byService.keySet).contains(Set("Amazon Elastic Container Service", "Amazon Simple Storage Service")),
          asked.get("TimePeriod").contains(Json.Obj("Start" -> Json.Str("2026-10-03"), "End" -> Json.Str("2026-11-02"))),
          asked.get("Filter").flatMap(_.asObject).flatMap(_.get("And")).flatMap(_.asArray).map(_.size).contains(2),
          found == List(CostAnomaly("arn:aws:ce::1:anomalymonitor/m/a1", Some(LocalDate.parse("2026-10-30").nn), Some("AmazonEC2"), 42.5,
            List("Amazon Elastic Compute Cloud - Compute / us-east-1"))),
        ),
  ).provideShared(Client.default)
