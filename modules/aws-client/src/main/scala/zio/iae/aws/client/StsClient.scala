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

object StsClient:
  val layer: ZLayer[AwsQueryClient, Nothing, StsClient] = ZLayer.fromFunction(StsClient.apply)

  private val protocol = AwsQueryProtocol(AwsService("sts", "sts"), "2011-06-15")
