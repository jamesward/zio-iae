package zio.iae.aws.client

import zio.*

opaque type AccountId = String

object AccountId:
  def from(value: String): Either[String, AccountId] = Either.cond(value.length == 12 && value.forall(_.isDigit), value, s"'$value' is not an AWS account id")

  extension (id: AccountId) def value: String = id

  given CanEqual[AccountId, AccountId] = CanEqual.derived

final case class StsClient(query: AwsQueryClient):
  def accountId: IO[AwsError, AccountId] =
    query
      .call(StsClient.protocol, "GetCallerIdentity", Nil)
      .flatMap: elem =>
        ZIO.fromEither((elem \\ "Account").headOption.toRight("no Account in response").flatMap(n => AccountId.from(n.text.trim)))
          .mapError(AwsError.Decode("sts", _))

  /** Temporary credentials for a role, and when they expire. */
  def assumeRole(roleArn: String, sessionName: String, duration: Duration): IO[AwsError, (AwsCredentials, Option[java.time.Instant])] =
    query
      .call(StsClient.protocol, "AssumeRole", List("RoleArn" -> roleArn, "RoleSessionName" -> sessionName, "DurationSeconds" -> duration.toSeconds.toString))
      .flatMap: elem =>
        def text(name: String) = (elem \\ name).headOption.map(_.text.trim).filter(_.nonEmpty)
        val parsed =
          for
            key    <- text("AccessKeyId").toRight("no AccessKeyId in response")
            secret <- text("SecretAccessKey").toRight("no SecretAccessKey in response")
          yield (
            AwsCredentials(AccessKeyId(key), SecretAccessKey(secret), text("SessionToken").map(SessionToken(_))),
            text("Expiration").flatMap(e => scala.util.Try(java.time.Instant.parse(e).nn).toOption),
          )
        ZIO.fromEither(parsed).mapError(AwsError.Decode("sts", _))

object StsClient:
  /** A config whose credentials come from assuming `roleArn` with `base`'s credentials, refreshed before expiry. */
  def assumed(base: AwsConfig, http: zio.http.Client, roleArn: String, sessionName: String, region: Option[Region] = None): UIO[AwsConfig] =
    val sts = StsClient(AwsQueryClient(http, base))
    CredentialsProvider
      .refreshing(sts.assumeRole(roleArn, sessionName, 1.hour))
      .map(provider => AwsConfig(region.getOrElse(base.region), provider, base.endpoint))

  val layer: ZLayer[AwsQueryClient, Nothing, StsClient] = ZLayer.fromFunction(StsClient.apply)

  private val protocol = AwsQueryProtocol(AwsService("sts", "sts"), "2011-06-15")
