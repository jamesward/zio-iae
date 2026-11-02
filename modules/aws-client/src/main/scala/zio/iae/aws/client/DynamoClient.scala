package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

final case class DynamoTable(name: String, arn: String, status: String, billingMode: Option[String], created: Option[java.time.Instant] = None)

/** DynamoDB tables with a single string partition key `pk` and one string attribute `v`: what typed key-value
  * tables need.
  */
final case class DynamoClient(json: AwsJsonClient):
  import DynamoClient.*

  def describe(name: String): IO[AwsError, Option[DynamoTable]] =
    call[TableRequest, DescribeResponse]("DescribeTable", TableRequest(name))
      .map(r => Some(tableOf(r.table)))
      .catchSome { case e if e.errorCode.contains("ResourceNotFoundException") => ZIO.none }

  def create(name: String, tags: Map[String, String]): IO[AwsError, Unit] =
    val request = CreateRequest(
      name,
      List(AttributeDefinition(keyAttribute, "S")),
      List(KeySchemaElement(keyAttribute, "HASH")),
      "PAY_PER_REQUEST",
      tagList(tags),
    )
    call[CreateRequest, AwsJsonClient.Empty]("CreateTable", request).unit

  /** Waits until a new table can take reads and writes. */
  def awaitActive(name: String): IO[AwsError, Unit] =
    describe(name)
      .repeat(Schedule.spaced(200.millis) && Schedule.recurUntil[Option[DynamoTable]](_.exists(_.status == "ACTIVE")) && Schedule.recurs(150))
      .unit

  def tags(arn: String): IO[AwsError, Map[String, String]] =
    call[ArnRequest, TagsResponse]("ListTagsOfResource", ArnRequest(arn)).map(_.tags.getOrElse(Nil).map(t => t.key -> t.value).toMap)

  def tag(arn: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call[TagRequest, AwsJsonClient.Empty]("TagResource", TagRequest(arn, tagList(tags))).unit.when(tags.nonEmpty).unit

  def untag(arn: String, keys: Set[String]): IO[AwsError, Unit] =
    call[UntagRequest, AwsJsonClient.Empty]("UntagResource", UntagRequest(arn, keys.toList.sorted)).unit.when(keys.nonEmpty).unit

  def delete(name: String): IO[AwsError, Unit] =
    call[TableRequest, AwsJsonClient.Empty]("DeleteTable", TableRequest(name)).unit
      .catchSome { case e if e.errorCode.contains("ResourceNotFoundException") => ZIO.unit }

  def put(name: String, key: String, value: String): IO[AwsError, Unit] =
    call[PutRequest, AwsJsonClient.Empty]("PutItem", PutRequest(name, Map(keyAttribute -> StringValue(key), valueAttribute -> StringValue(value)))).unit

  def get(name: String, key: String): IO[AwsError, Option[String]] =
    call[GetRequest, GetResponse]("GetItem", GetRequest(name, Map(keyAttribute -> StringValue(key)), consistentRead = true))
      .map(_.item.flatMap(_.get(valueAttribute)).map(_.s))

  def deleteItem(name: String, key: String): IO[AwsError, Unit] =
    call[KeyRequest, AwsJsonClient.Empty]("DeleteItem", KeyRequest(name, Map(keyAttribute -> StringValue(key)))).unit

  private def call[Req: Schema, Res: Schema](operation: String, request: Req): IO[AwsError, Res] = json.call[Req, Res](protocol, operation, request)

object DynamoClient:
  val layer: ZLayer[AwsJsonClient, Nothing, DynamoClient] = ZLayer.fromFunction(DynamoClient.apply)

  val keyAttribute   = "pk"
  val valueAttribute = "v"

  private val protocol = AwsJsonProtocol(AwsService("dynamodb", "dynamodb"), "DynamoDB_20120810", "1.0")

  private def tagList(tags: Map[String, String]): List[Tag] = tags.toList.sorted.map(Tag(_, _))

  private def tableOf(t: TableDescription): DynamoTable = DynamoTable(t.name, t.arn, t.status, t.billing.map(_.mode), t.created.map(s => java.time.Instant.ofEpochMilli((s * 1000).toLong).nn))

  private final case class Tag(@fieldName("Key") key: String, @fieldName("Value") value: String) derives Schema
  private final case class StringValue(@fieldName("S") s: String) derives Schema
  private final case class AttributeDefinition(@fieldName("AttributeName") name: String, @fieldName("AttributeType") tpe: String) derives Schema
  private final case class KeySchemaElement(@fieldName("AttributeName") name: String, @fieldName("KeyType") keyType: String) derives Schema
  private final case class TableRequest(@fieldName("TableName") name: String) derives Schema
  private final case class BillingSummary(@fieldName("BillingMode") mode: String) derives Schema
  private final case class TableDescription(
      @fieldName("TableName") name: String,
      @fieldName("TableArn") arn: String,
      @fieldName("TableStatus") status: String,
      @fieldName("BillingModeSummary") billing: Option[BillingSummary],
      @fieldName("CreationDateTime") created: Option[Double] = None,
  ) derives Schema
  private final case class DescribeResponse(@fieldName("Table") table: TableDescription) derives Schema
  private final case class CreateRequest(
      @fieldName("TableName") name: String,
      @fieldName("AttributeDefinitions") attributes: List[AttributeDefinition],
      @fieldName("KeySchema") keySchema: List[KeySchemaElement],
      @fieldName("BillingMode") billingMode: String,
      @fieldName("Tags") tags: List[Tag],
  ) derives Schema
  private final case class ArnRequest(@fieldName("ResourceArn") arn: String) derives Schema
  private final case class TagsResponse(@fieldName("Tags") tags: Option[List[Tag]]) derives Schema
  private final case class TagRequest(@fieldName("ResourceArn") arn: String, @fieldName("Tags") tags: List[Tag]) derives Schema
  private final case class UntagRequest(@fieldName("ResourceArn") arn: String, @fieldName("TagKeys") keys: List[String]) derives Schema
  private final case class PutRequest(@fieldName("TableName") name: String, @fieldName("Item") item: Map[String, StringValue]) derives Schema
  private final case class GetRequest(
      @fieldName("TableName") name: String,
      @fieldName("Key") key: Map[String, StringValue],
      @fieldName("ConsistentRead") consistentRead: Boolean,
  ) derives Schema
  private final case class GetResponse(@fieldName("Item") item: Option[Map[String, StringValue]]) derives Schema
  private final case class KeyRequest(@fieldName("TableName") name: String, @fieldName("Key") key: Map[String, StringValue]) derives Schema
