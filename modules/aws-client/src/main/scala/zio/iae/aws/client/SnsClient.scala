package zio.iae.aws.client

import scala.xml.Elem
import zio.*
import zio.stream.ZStream

final case class SnsSubscription(arn: String, protocol: String, endpoint: String)

/** SNS topics, their tags and SQS subscriptions, and publishing. */
final case class SnsClient(query: AwsQueryClient):
  import SnsClient.*

  def createTopic(name: String, tags: Map[String, String]): IO[AwsError, String] =
    call("CreateTopic", ("Name" -> name) :: tagParams(tags)).flatMap(text(_, "TopicArn"))

  def exists(arn: String): IO[AwsError, Boolean] =
    call("GetTopicAttributes", List("TopicArn" -> arn)).as(true).catchSome:
      case e if e.errorCode.exists(c => c == "NotFound" || c == "NotFoundException") => ZIO.succeed(false)

  def tags(arn: String): IO[AwsError, Map[String, String]] =
    call("ListTagsForResource", List("ResourceArn" -> arn)).map(elem => (elem \\ "Tags" \ "member").map(m => (m \ "Key").text.trim -> (m \ "Value").text.trim).toMap)

  def tag(arn: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call("TagResource", ("ResourceArn" -> arn) :: tagParams(tags)).unit.when(tags.nonEmpty).unit

  def untag(arn: String, keys: Set[String]): IO[AwsError, Unit] =
    val params = keys.toList.sorted.zipWithIndex.map((k, i) => s"TagKeys.member.${i + 1}" -> k)
    call("UntagResource", ("ResourceArn" -> arn) :: params).unit.when(keys.nonEmpty).unit

  def subscriptions(arn: String): IO[AwsError, List[SnsSubscription]] =
    ZStream
      .paginateChunkZIO(Option.empty[String]): token =>
        call("ListSubscriptionsByTopic", ("TopicArn" -> arn) :: token.map("NextToken" -> _).toList).map: elem =>
          val subs = Chunk.fromIterable((elem \\ "Subscriptions" \ "member").map(m => SnsSubscription((m \ "SubscriptionArn").text.trim, (m \ "Protocol").text.trim, (m \ "Endpoint").text.trim)))
          (subs, (elem \\ "NextToken").headOption.map(_.text.trim).filter(_.nonEmpty).map(Some(_)))
      .runCollect
      .map(_.toList)

  /** Subscribes an SQS queue with raw delivery, so the queue receives the message exactly as published. */
  def subscribeQueue(topicArn: String, queueArn: String): IO[AwsError, Unit] =
    call(
      "Subscribe",
      List(
        "TopicArn"                    -> topicArn,
        "Protocol"                    -> "sqs",
        "Endpoint"                    -> queueArn,
        "Attributes.entry.1.key"      -> "RawMessageDelivery",
        "Attributes.entry.1.value"    -> "true",
        "ReturnSubscriptionArn"       -> "true",
      ),
    ).unit

  def unsubscribe(subscriptionArn: String): IO[AwsError, Unit] = call("Unsubscribe", List("SubscriptionArn" -> subscriptionArn)).unit

  def publish(arn: String, message: String): IO[AwsError, Unit] = call("Publish", List("TopicArn" -> arn, "Message" -> message)).unit

  def deleteTopic(arn: String): IO[AwsError, Unit] = call("DeleteTopic", List("TopicArn" -> arn)).unit

  private def call(action: String, params: List[(String, String)]): IO[AwsError, Elem] = query.call(protocol, action, params)

object SnsClient:
  val layer: ZLayer[AwsQueryClient, Nothing, SnsClient] = ZLayer.fromFunction(SnsClient.apply)

  private val protocol = AwsQueryProtocol(AwsService("sns", "sns"), "2010-03-31")

  private def text(elem: Elem, label: String): IO[AwsError, String] =
    ZIO.fromOption((elem \\ label).headOption.map(_.text.trim)).orElseFail(AwsError.Decode("sns", s"no $label in response"))

  private def tagParams(tags: Map[String, String]): List[(String, String)] =
    tags.toList.sorted.zipWithIndex.flatMap((tag, i) => List(s"Tags.member.${i + 1}.Key" -> tag._1, s"Tags.member.${i + 1}.Value" -> tag._2))
