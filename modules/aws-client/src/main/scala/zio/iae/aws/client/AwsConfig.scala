package zio.iae.aws.client

import java.net.URI
import scala.util.Try
import zio.*

opaque type Region = String

object Region:
  private val pattern = "[a-z]{2}(-[a-z]+)+-[0-9]+".r

  def from(value: String): Either[String, Region] =
    Either.cond(pattern.matches(value), value, s"'$value' is not an AWS region name")

  val UsEast1: Region = "us-east-1"

  extension (region: Region) def value: String = region

  given CanEqual[Region, Region] = CanEqual.derived

final case class AccessKeyId(value: String)

final case class SecretAccessKey(value: String):
  override def toString: String = "SecretAccessKey(***)"

final case class SessionToken(value: String):
  override def toString: String = "SessionToken(***)"

final case class AwsCredentials(accessKeyId: AccessKeyId, secretAccessKey: SecretAccessKey, sessionToken: Option[SessionToken])

enum Scheme derives CanEqual:
  case Http, Https

  def value: String = this match
    case Http  => "http"
    case Https => "https"

/** Where to send requests instead of the public AWS endpoints, e.g. Floci. Requests then use path-style addressing. */
final case class Endpoint(scheme: Scheme, host: String, port: Option[Int]):
  def authority: String = port.fold(host)(p => s"$host:$p")

object Endpoint:
  def parse(value: String): Either[String, Endpoint] =
    for
      uri    <- Try(URI(value)).toEither.left.map(e => s"'$value' is not a URI: ${e.getMessage}")
      scheme <- Option(uri.getScheme).map(_.toLowerCase).collect {
                  case "http"  => Scheme.Http
                  case "https" => Scheme.Https
                }.toRight(s"'$value' must start with http:// or https://")
      host   <- Option(uri.getHost).toRight(s"'$value' has no host")
    yield Endpoint(scheme, host, Option.when(uri.getPort >= 0)(uri.getPort))

final case class AwsConfig(region: Region, credentials: AwsCredentials, endpoint: Option[Endpoint])

final case class AwsConfigError(detail: String):
  def message: String = s"AWS configuration: $detail"

object AwsConfig:
  /** Reads the standard AWS environment variables. Only static credentials are supported so far. */
  val fromEnvironment: IO[AwsConfigError, AwsConfig] =
    for
      regionName <- firstOf("AWS_REGION", "AWS_DEFAULT_REGION")
      region     <- ZIO.fromEither(Region.from(regionName)).mapError(AwsConfigError(_))
      keyId      <- required("AWS_ACCESS_KEY_ID")
      secret     <- required("AWS_SECRET_ACCESS_KEY")
      token      <- optional("AWS_SESSION_TOKEN")
      endpointS  <- optional("AWS_ENDPOINT_URL")
      endpoint   <- ZIO.foreach(endpointS)(s => ZIO.fromEither(Endpoint.parse(s)).mapError(AwsConfigError(_)))
    yield AwsConfig(
      region,
      AwsCredentials(AccessKeyId(keyId), SecretAccessKey(secret), token.map(SessionToken(_))),
      endpoint,
    )

  val layer: ZLayer[Any, AwsConfigError, AwsConfig] = ZLayer(fromEnvironment)

  private def optional(name: String): IO[AwsConfigError, Option[String]] =
    System.env(name).mapError(e => AwsConfigError(s"reading $name: ${e.getMessage}")).map(_.filter(_.nonEmpty))

  private def required(name: String): IO[AwsConfigError, String] =
    optional(name).someOrFail(AwsConfigError(s"$name is not set"))

  private def firstOf(names: String*): IO[AwsConfigError, String] =
    ZIO
      .foreach(names)(optional)
      .map(_.flatten.headOption)
      .someOrFail(AwsConfigError(s"none of ${names.mkString(", ")} is set"))
