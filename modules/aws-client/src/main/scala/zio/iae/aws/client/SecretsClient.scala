package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName

final case class SecretDescription(arn: String, name: String, description: Option[String], tags: Map[String, String])

/** Secrets Manager: the secret's lifecycle and tags for the provider, reading the value for apps. */
final case class SecretsClient(json: AwsJsonClient):
  import SecretsClient.*

  def describe(name: String): IO[AwsError, Option[SecretDescription]] =
    call[SecretIdRequest, DescribeResponse]("DescribeSecret", SecretIdRequest(name))
      .map(r => Some(SecretDescription(r.arn, r.name, r.description.filter(_.nonEmpty), r.tags.getOrElse(Nil).map(t => t.key -> t.value).toMap)))
      .catchSome { case e if e.errorCode.contains("ResourceNotFoundException") => ZIO.none }

  def create(name: String, description: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call[CreateRequest, AwsJsonClient.Empty]("CreateSecret", CreateRequest(name, description, tagList(tags))).unit

  def updateDescription(name: String, description: String): IO[AwsError, Unit] =
    call[UpdateRequest, AwsJsonClient.Empty]("UpdateSecret", UpdateRequest(name, description)).unit

  def tag(name: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call[TagRequest, AwsJsonClient.Empty]("TagResource", TagRequest(name, tagList(tags))).unit.when(tags.nonEmpty).unit

  def untag(name: String, keys: Set[String]): IO[AwsError, Unit] =
    call[UntagRequest, AwsJsonClient.Empty]("UntagResource", UntagRequest(name, keys.toList.sorted)).unit.when(keys.nonEmpty).unit

  /** `None` when the secret exists but has no value yet. */
  def value(name: String): IO[AwsError, Option[String]] =
    call[SecretIdRequest, ValueResponse]("GetSecretValue", SecretIdRequest(name))
      .map(_.secretString)
      .catchSome { case e if e.errorCode.contains("ResourceNotFoundException") => ZIO.none }

  def putValue(name: String, value: String): IO[AwsError, Unit] =
    call[PutValueRequest, AwsJsonClient.Empty]("PutSecretValue", PutValueRequest(name, value)).unit

  def delete(name: String): IO[AwsError, Unit] =
    call[DeleteRequest, AwsJsonClient.Empty]("DeleteSecret", DeleteRequest(name, 30)).unit
      .catchSome { case e if e.errorCode.contains("ResourceNotFoundException") => ZIO.unit }

  private def call[Req: Schema, Res: Schema](operation: String, request: Req): IO[AwsError, Res] = json.call[Req, Res](protocol, operation, request)

object SecretsClient:
  val layer: ZLayer[AwsJsonClient, Nothing, SecretsClient] = ZLayer.fromFunction(SecretsClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("secretsmanager", "secretsmanager"), "secretsmanager", "1.1")

  private def tagList(tags: Map[String, String]): List[Tag] = tags.toList.sorted.map(Tag(_, _))

  private final case class Tag(@fieldName("Key") key: String, @fieldName("Value") value: String) derives Schema
  private final case class SecretIdRequest(@fieldName("SecretId") secretId: String) derives Schema
  private final case class DescribeResponse(
      @fieldName("ARN") arn: String,
      @fieldName("Name") name: String,
      @fieldName("Description") description: Option[String],
      @fieldName("Tags") tags: Option[List[Tag]],
  ) derives Schema
  private final case class CreateRequest(@fieldName("Name") name: String, @fieldName("Description") description: String, @fieldName("Tags") tags: List[Tag])
      derives Schema
  private final case class UpdateRequest(@fieldName("SecretId") secretId: String, @fieldName("Description") description: String) derives Schema
  private final case class TagRequest(@fieldName("SecretId") secretId: String, @fieldName("Tags") tags: List[Tag]) derives Schema
  private final case class UntagRequest(@fieldName("SecretId") secretId: String, @fieldName("TagKeys") keys: List[String]) derives Schema
  private final case class ValueResponse(@fieldName("SecretString") secretString: Option[String]) derives Schema
  private final case class PutValueRequest(@fieldName("SecretId") secretId: String, @fieldName("SecretString") secretString: String) derives Schema
  private final case class DeleteRequest(@fieldName("SecretId") secretId: String, @fieldName("RecoveryWindowInDays") recoveryWindowInDays: Int)
      derives Schema
