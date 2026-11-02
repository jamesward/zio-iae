package zio.iae.aws

import java.time.LocalDate
import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** The bill from Cost Explorer: an app's cost in an env by its `iae:app` and `iae:env` cost-allocation tags (which
  * `iae bootstrap` activates), and Cost Anomaly Detection's anomalies in the env's account.
  */
final case class CostExplorerBills(ce: CostExplorerClient) extends Bills:
  def spend(app: AppName, env: EnvName, from: LocalDate, to: LocalDate): IO[ProviderError, Option[Spend]] =
    ce.costs(Map(TagKey.App.wire -> app.value, TagKey.Env.wire -> env.value), from, to)
      .mapBoth(failed(app, env, "read costs"), c => Some(Spend(c.from, c.to, c.total, c.byService)))

  def anomalies(env: EnvName, since: LocalDate): IO[ProviderError, List[CostAnomaly]] =
    Clock.localDateTime.map(_.toLocalDate.nn).flatMap: today =>
      ce.anomalies(since, today)
        .mapBoth(failed(AppName("billing"), env, "read cost anomalies"), _.map(a => CostAnomaly(a.id, a.start, a.dimension, a.impact, a.causes)))

  private def failed(app: AppName, env: EnvName, operation: String)(e: AwsError): ProviderError =
    ProviderError(Address(app, env, ResourceKind.Bucket, LogicalId("billing")), operation, e.message)
