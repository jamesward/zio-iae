package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

/** The AWS Price List API (`GetProducts`): public on-demand prices by service and product attributes. One endpoint in
  * us-east-1 for every region's prices.
  */
final case class PriceListClient(json: AwsJsonClient):
  import PriceListClient.*

  /** The products (each one product's JSON, attributes and terms) matching the filters: `(type, field, value)`, e.g.
    * `("TERM_MATCH", "regionCode", "us-east-1")` or `("CONTAINS", "usagetype", "Fargate-vCPU-Hours")`.
    */
  def products(serviceCode: String, filters: List[(String, String, String)]): IO[AwsError, List[String]] =
    def page(token: Option[String], left: Int): IO[AwsError, List[String]] =
      json
        .call[Request, Response](protocol, "GetProducts", Request(serviceCode, filters.map(Filter.apply.tupled), "aws_v1", token, 100))
        .flatMap: r =>
          val found = r.priceList.getOrElse(Nil)
          r.next.filter(t => t.nonEmpty && left > 1).fold(ZIO.succeed(found))(t => page(Some(t), left - 1).map(found ++ _))
    page(None, 10)

object PriceListClient:
  val layer: ZLayer[AwsJsonClient, Nothing, PriceListClient] = ZLayer.fromFunction(PriceListClient.apply)

  /** `api.pricing.us-east-1.amazonaws.com`, signed for us-east-1. */
  private val protocol = AwsJsonProtocol(AwsService("pricing", "api.pricing.us-east-1", global = true), "AWSPriceListService", "1.1")

  final case class Filter(@fieldName("Type") tpe: String, @fieldName("Field") field: String, @fieldName("Value") value: String) derives Schema
  final case class Request(
      @fieldName("ServiceCode") serviceCode: String,
      @fieldName("Filters") filters: List[Filter],
      @fieldName("FormatVersion") formatVersion: String,
      @fieldName("NextToken") next: Option[String],
      @fieldName("MaxResults") max: Int,
  ) derives Schema
  final case class Response(@fieldName("PriceList") priceList: Option[List[String]], @fieldName("NextToken") next: Option[String]) derives Schema
