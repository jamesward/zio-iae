package zio.iae.aws

import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.aws.client.*
import zio.schema.codec.JsonCodec

/** Unused permissions of an app's runtime role, from IAM's Access Advisor: a statement of the role's generated policy
  * (one per grant) is unused when none of its actions was used since the cutoff, judged per action where IAM tracks
  * actions (S3) and per service otherwise. A role created after the cutoff is left alone.
  */
final case class IamAccess(iam: IamClient) extends Access:
  def unused(app: AppName, env: EnvName, since: Instant): IO[ProviderError, List[UnusedPermission]] =
    val address = Address(app, env, ResourceKind.Role, Manifest.runtimeRole)
    def failed(e: AwsError) = ProviderError(address, "read access", e.message)
    ZIO.fromEither(RoleProvider.roleName(app, env)).mapError(ProviderError(address, "name", _)).flatMap: name =>
      iam.getRole(name).mapError(failed).flatMap:
        case Some(role) if role.created.forall(_.isBefore(since)) =>
          for
            policy <- iam.getInlinePolicy(name, RoleProvider.policyName).mapError(failed)
            doc     = policy.flatMap(p => JsonCodec.jsonDecoder(zio.schema.Schema[PolicyDocument]).decodeJson(p).toOption)
            usage  <- iam.lastAccessed(role.arn).mapError(failed)
          yield doc.toList.flatMap(_.statements).filterNot(s => s.actions.exists(IamAccess.usedSince(usage, _, since)))
            .map(s => UnusedPermission(s.sid, s.actions.sorted, s.resources))
        case _ => ZIO.succeed(Nil)

object IamAccess:
  /** Whether an action (`s3:PutObject`) was used since a time: its tracked action if IAM tracks it, else its service. */
  def usedSince(usage: List[ServiceAccess], action: String, since: Instant): Boolean =
    val (namespace, name) = action.split(':').toList match
      case n :: a :: Nil => (n, a)
      case _             => (action, action)
    usage.find(_.namespace == namespace).exists: service =>
      val last = service.actions.get(name).getOrElse(service.lastUsed)
      last.exists(!_.isBefore(since))
