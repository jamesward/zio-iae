package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

opaque type QueueUrl = String

object QueueUrl:
  def apply(value: String): QueueUrl = value

  extension (url: QueueUrl) def value: String = url

  given CanEqual[QueueUrl, QueueUrl] = CanEqual.derived

final case class SqsMessage(id: String, receipt: String, body: String, receiveCount: Option[Int])

/** SQS over its JSON protocol: the queue lifecycle the provider needs and the messaging the capabilities need. */
final case class SqsClient(json: AwsJsonClient):
  import SqsClient.*

  def queueUrl(name: String): IO[AwsError, Option[QueueUrl]] =
    call[GetQueueUrlRequest, QueueUrlResponse]("GetQueueUrl", GetQueueUrlRequest(name))
      .map(r => Some(QueueUrl(r.queueUrl)))
      .catchSome { case e if e.errorCode.exists(nonExistent) => ZIO.none }

  def createQueue(name: String, attributes: Map[String, String], tags: Map[String, String]): IO[AwsError, QueueUrl] =
    call[CreateQueueRequest, QueueUrlResponse]("CreateQueue", CreateQueueRequest(name, attributes, tags)).map(r => QueueUrl(r.queueUrl))

  def attributes(url: QueueUrl): IO[AwsError, Map[String, String]] =
    call[GetAttributesRequest, AttributesResponse]("GetQueueAttributes", GetAttributesRequest(url.value, List("All"))).map(_.attributes.getOrElse(Map.empty))

  def setAttributes(url: QueueUrl, attributes: Map[String, String]): IO[AwsError, Unit] =
    call[SetAttributesRequest, AwsJsonClient.Empty]("SetQueueAttributes", SetAttributesRequest(url.value, attributes)).unit.when(attributes.nonEmpty).unit

  def tags(url: QueueUrl): IO[AwsError, Map[String, String]] =
    call[QueueRequest, TagsResponse]("ListQueueTags", QueueRequest(url.value)).map(_.tags.getOrElse(Map.empty))

  def tag(url: QueueUrl, tags: Map[String, String]): IO[AwsError, Unit] =
    call[TagRequest, AwsJsonClient.Empty]("TagQueue", TagRequest(url.value, tags)).unit.when(tags.nonEmpty).unit

  def untag(url: QueueUrl, keys: Set[String]): IO[AwsError, Unit] =
    call[UntagRequest, AwsJsonClient.Empty]("UntagQueue", UntagRequest(url.value, keys.toList.sorted)).unit.when(keys.nonEmpty).unit

  def deleteQueue(url: QueueUrl): IO[AwsError, Unit] =
    call[QueueRequest, AwsJsonClient.Empty]("DeleteQueue", QueueRequest(url.value)).unit
      .catchSome { case e if e.errorCode.exists(nonExistent) => ZIO.unit }

  def send(url: QueueUrl, body: String): IO[AwsError, String] =
    call[SendRequest, SendResponse]("SendMessage", SendRequest(url.value, body)).map(_.messageId)

  def receive(url: QueueUrl, max: Int, waitSeconds: Int): IO[AwsError, Chunk[SqsMessage]] =
    call[ReceiveRequest, ReceiveResponse]("ReceiveMessage", ReceiveRequest(url.value, max, waitSeconds, List("ApproximateReceiveCount")))
      .map: r =>
        Chunk.fromIterable(r.messages.getOrElse(Nil)).map: m =>
          SqsMessage(m.messageId, m.receiptHandle, m.body, m.attributes.flatMap(_.get("ApproximateReceiveCount")).flatMap(_.toIntOption))

  def delete(url: QueueUrl, receipt: String): IO[AwsError, Unit] =
    call[DeleteMessageRequest, AwsJsonClient.Empty]("DeleteMessage", DeleteMessageRequest(url.value, receipt)).unit

  private def call[Req: Schema, Res: Schema](operation: String, request: Req): IO[AwsError, Res] = json.call[Req, Res](protocol, operation, request)

object SqsClient:
  val layer: ZLayer[AwsJsonClient, Nothing, SqsClient] = ZLayer.fromFunction(SqsClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("sqs", "sqs"), "AmazonSQS", "1.0")

  private def nonExistent(code: String): Boolean = code == "QueueDoesNotExist" || code.endsWith("NonExistentQueue")

  private final case class GetQueueUrlRequest(@fieldName("QueueName") queueName: String) derives Schema
  private final case class QueueUrlResponse(@fieldName("QueueUrl") queueUrl: String) derives Schema
  private final case class CreateQueueRequest(
      @fieldName("QueueName") queueName: String,
      @fieldName("Attributes") attributes: Map[String, String],
      @fieldName("tags") tags: Map[String, String],
  ) derives Schema
  private final case class QueueRequest(@fieldName("QueueUrl") queueUrl: String) derives Schema
  private final case class GetAttributesRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("AttributeNames") names: List[String]) derives Schema
  private final case class AttributesResponse(@fieldName("Attributes") attributes: Option[Map[String, String]]) derives Schema
  private final case class SetAttributesRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("Attributes") attributes: Map[String, String]) derives Schema
  private final case class TagsResponse(@fieldName("Tags") tags: Option[Map[String, String]]) derives Schema
  private final case class TagRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("Tags") tags: Map[String, String]) derives Schema
  private final case class UntagRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("TagKeys") keys: List[String]) derives Schema
  private final case class SendRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("MessageBody") body: String) derives Schema
  private final case class SendResponse(@fieldName("MessageId") messageId: String) derives Schema
  private final case class ReceiveRequest(
      @fieldName("QueueUrl") queueUrl: String,
      @fieldName("MaxNumberOfMessages") max: Int,
      @fieldName("WaitTimeSeconds") waitSeconds: Int,
      @fieldName("MessageSystemAttributeNames") attributeNames: List[String],
  ) derives Schema
  private final case class Message(
      @fieldName("MessageId") messageId: String,
      @fieldName("ReceiptHandle") receiptHandle: String,
      @fieldName("Body") body: String,
      @fieldName("Attributes") attributes: Option[Map[String, String]],
  ) derives Schema
  private final case class ReceiveResponse(@fieldName("Messages") messages: Option[List[Message]]) derives Schema
  private final case class DeleteMessageRequest(@fieldName("QueueUrl") queueUrl: String, @fieldName("ReceiptHandle") receipt: String) derives Schema
