package zio.iae.aws.client

import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.xml.{Elem, Node}
import zio.*
import zio.stream.ZStream

opaque type RoleName = String

object RoleName:
  private val pattern = "[A-Za-z0-9+=,.@_-]{1,64}".r

  def from(value: String): Either[String, RoleName] = Either.cond(pattern.matches(value), value, s"'$value' is not a valid IAM role name")

  extension (name: RoleName) def value: String = name

  given CanEqual[RoleName, RoleName] = CanEqual.derived

final case class IamRole(name: RoleName, path: String, arn: String, trustPolicy: Option[String])

/** IAM roles and their inline policies and tags. IAM is global: one endpoint, signed for us-east-1. */
final case class IamClient(query: AwsQueryClient):
  import IamClient.*

  def getRole(name: RoleName): IO[AwsError, Option[IamRole]] =
    call("GetRole", List("RoleName" -> name.value))
      .map(elem => (elem \\ "Role").headOption.flatMap(roleOf))
      .catchSome { case e if e.errorCode.contains("NoSuchEntity") => ZIO.none }

  def createRole(name: RoleName, path: String, trustPolicy: String, tags: Map[String, String]): IO[AwsError, Unit] =
    call("CreateRole", List("RoleName" -> name.value, "Path" -> path, "AssumeRolePolicyDocument" -> trustPolicy) ++ tagParams(tags)).unit

  def updateTrustPolicy(name: RoleName, trustPolicy: String): IO[AwsError, Unit] =
    call("UpdateAssumeRolePolicy", List("RoleName" -> name.value, "PolicyDocument" -> trustPolicy)).unit

  def getInlinePolicy(name: RoleName, policyName: String): IO[AwsError, Option[String]] =
    call("GetRolePolicy", List("RoleName" -> name.value, "PolicyName" -> policyName))
      .map(elem => (elem \\ "PolicyDocument").headOption.map(n => decode(n.text)))
      .catchSome { case e if e.errorCode.contains("NoSuchEntity") => ZIO.none }

  def putInlinePolicy(name: RoleName, policyName: String, document: String): IO[AwsError, Unit] =
    call("PutRolePolicy", List("RoleName" -> name.value, "PolicyName" -> policyName, "PolicyDocument" -> document)).unit

  def deleteInlinePolicy(name: RoleName, policyName: String): IO[AwsError, Unit] =
    call("DeleteRolePolicy", List("RoleName" -> name.value, "PolicyName" -> policyName)).unit
      .catchSome { case e if e.errorCode.contains("NoSuchEntity") => ZIO.unit }

  def deleteRole(name: RoleName): IO[AwsError, Unit] =
    call("DeleteRole", List("RoleName" -> name.value)).unit.catchSome { case e if e.errorCode.contains("NoSuchEntity") => ZIO.unit }

  def roleTags(name: RoleName): IO[AwsError, Map[String, String]] =
    call("ListRoleTags", List("RoleName" -> name.value)).map(elem => tagsOf(elem \\ "Tags"))

  def tagRole(name: RoleName, tags: Map[String, String]): IO[AwsError, Unit] =
    call("TagRole", ("RoleName" -> name.value) :: tagParams(tags)).unit.when(tags.nonEmpty).unit

  def untagRole(name: RoleName, keys: Set[String]): IO[AwsError, Unit] =
    val params = keys.toList.sorted.zipWithIndex.map((k, i) => s"TagKeys.member.${i + 1}" -> k)
    call("UntagRole", ("RoleName" -> name.value) :: params).unit.when(keys.nonEmpty).unit

  def listRoles(pathPrefix: String): ZStream[Any, AwsError, IamRole] =
    ZStream.paginateChunkZIO(Option.empty[String]): marker =>
      call("ListRoles", List("PathPrefix" -> pathPrefix) ++ marker.map("Marker" -> _)).map: elem =>
        val roles     = Chunk.fromIterable((elem \\ "Roles" \ "member").flatMap(roleOf))
        val truncated = (elem \\ "IsTruncated").headOption.exists(_.text.trim == "true")
        (roles, (elem \\ "Marker").headOption.map(_.text.trim).filter(_ => truncated).map(Some(_)))

  private def call(action: String, params: List[(String, String)]): IO[AwsError, Elem] = query.call(protocol, action, params)

object IamClient:
  val layer: ZLayer[AwsQueryClient, Nothing, IamClient] = ZLayer.fromFunction(IamClient.apply)

  private val protocol = AwsQueryProtocol(AwsService("iam", "iam", global = true), "2010-05-08")

  private def roleOf(node: Node): Option[IamRole] =
    RoleName.from((node \ "RoleName").text.trim).toOption.map: name =>
      IamRole(name, (node \ "Path").text.trim, (node \ "Arn").text.trim, (node \ "AssumeRolePolicyDocument").headOption.map(n => decode(n.text)))

  private def tagsOf(tags: scala.xml.NodeSeq): Map[String, String] =
    (tags \ "member").map(m => (m \ "Key").text.trim -> (m \ "Value").text.trim).toMap

  private def tagParams(tags: Map[String, String]): List[(String, String)] =
    tags.toList.sorted.zipWithIndex.flatMap: (tag, i) =>
      List(s"Tags.member.${i + 1}.Key" -> tag._1, s"Tags.member.${i + 1}.Value" -> tag._2)

  /** IAM returns policy documents URL-encoded. */
  private def decode(text: String): String =
    val trimmed = text.trim
    if trimmed.startsWith("{") then trimmed else URLDecoder.decode(trimmed, UTF_8).nn
