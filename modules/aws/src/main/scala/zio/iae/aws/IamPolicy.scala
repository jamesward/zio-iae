package zio.iae.aws

import zio.iae.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.schema.codec.JsonCodec

final case class Statement(
    @fieldName("Sid") sid: String,
    @fieldName("Effect") effect: String,
    @fieldName("Action") actions: List[String],
    @fieldName("Resource") resources: List[String],
) derives Schema, CanEqual

final case class PolicyDocument(@fieldName("Version") version: String, @fieldName("Statement") statements: List[Statement])
    derives Schema, CanEqual:
  /** Every (action, resource) pair the document allows: what widening and narrowing are measured in. */
  def permissions: Set[(String, String)] =
    statements.filter(_.effect == "Allow").flatMap(s => for a <- s.actions; r <- s.resources yield (a, r)).toSet

  def toJson: String = JsonCodec.jsonEncoder(Schema[PolicyDocument]).encodeJson(this, None).toString

final case class Principal(@fieldName("Service") services: List[String]) derives Schema, CanEqual

final case class TrustStatement(
    @fieldName("Effect") effect: String,
    @fieldName("Principal") principal: Principal,
    @fieldName("Action") actions: List[String],
) derives Schema, CanEqual

final case class TrustDocument(@fieldName("Version") version: String, @fieldName("Statement") statements: List[TrustStatement])
    derives Schema, CanEqual:
  def services: Set[String] = statements.flatMap(_.principal.services).toSet
  def toJson: String        = JsonCodec.jsonEncoder(Schema[TrustDocument]).encodeJson(this, None).toString

/** Least-privilege IAM from an app's grants: one statement per (resource, capability), scoped to that resource. */
object IamPolicy:
  val version = "2012-10-17"

  def permissions(app: AppName, env: EnvName, spec: RoleSpec, buckets: BucketNaming, context: AwsContext): Either[InfraError, PolicyDocument] =
    val statements = spec.grants.map: grant =>
      val sid = s"${grant.capability}${camel(grant.id)}"
      grant.kind match
        case ResourceKind.Bucket =>
          buckets.physical(Address(app, env, ResourceKind.Bucket, grant.id)).map: name =>
            val bucket  = s"arn:aws:s3:::${name.value}"
            val objects = s"$bucket/*"
            grant.capability match
              case Capability.BucketRead  => Statement(sid, "Allow", List("s3:GetObject", "s3:ListBucket"), List(bucket, objects))
              case _                      => Statement(sid, "Allow", List("s3:DeleteObject", "s3:PutObject"), List(objects))
        case ResourceKind.Queue =>
          QueueNaming.physical(Address(app, env, ResourceKind.Queue, grant.id)).map: name =>
            val queue = QueueNaming.arn(context, name)
            grant.capability match
              case Capability.QueueSend => Statement(sid, "Allow", List("sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:SendMessage"), List(queue))
              case _ =>
                Statement(
                  sid,
                  "Allow",
                  List("sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage"),
                  List(queue),
                )
        case ResourceKind.Role => Left(InfraError.InvalidConfig(s"a role can't be granted to an app: ${grant.id.value}"))
    statements.partitionMap(identity) match
      case (Nil, all)     => Right(PolicyDocument(version, all))
      case (error :: _, _) => Left(error)

  def trust(spec: RoleSpec): TrustDocument =
    val services = spec.trust.map:
      case TrustedService.EcsTasks => "ecs-tasks.amazonaws.com"
      case TrustedService.Lambda   => "lambda.amazonaws.com"
    TrustDocument(version, List(TrustStatement("Allow", Principal(services.sorted), List("sts:AssumeRole"))))

  def parsePermissions(json: String): Option[PolicyDocument] = JsonCodec.jsonDecoder(Schema[PolicyDocument]).decodeJson(json).toOption
  def parseTrust(json: String): Option[TrustDocument]        = JsonCodec.jsonDecoder(Schema[TrustDocument]).decodeJson(json).toOption

  private def camel(id: LogicalId): String = id.value.split('-').map(_.capitalize).mkString
