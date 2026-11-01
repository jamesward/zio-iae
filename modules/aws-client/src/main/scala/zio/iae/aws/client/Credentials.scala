package zio.iae.aws.client

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.time.Instant
import zio.*
import zio.json.*
import zio.json.ast.Json

/** Where request credentials come from. Temporary ones (an ECS task role, an assumed role) are refreshed before they
  * expire.
  */
trait CredentialsProvider:
  def credentials: IO[AwsError, AwsCredentials]

object CredentialsProvider:
  final case class Static(value: AwsCredentials) extends CredentialsProvider:
    val credentials: IO[AwsError, AwsCredentials] = ZIO.succeed(value)

  /** Caches `fetch`'s credentials until five minutes before they expire; credentials without an expiry are kept. */
  def refreshing(fetch: IO[AwsError, (AwsCredentials, Option[Instant])]): UIO[CredentialsProvider] =
    Ref.Synchronized.make(Option.empty[(AwsCredentials, Option[Instant])]).map: cache =>
      new CredentialsProvider:
        def credentials: IO[AwsError, AwsCredentials] =
          cache.modifyZIO: cached =>
            Clock.ClockLive.instant.flatMap: now =>
              cached match
                case Some((c, expiry)) if expiry.forall(_.minusSeconds(300).nn.isAfter(now)) => ZIO.succeed((c, cached))
                case _                                                                       => fetch.map(f => (f._1, Some(f)))

  /** The ECS task role: `AWS_CONTAINER_CREDENTIALS_FULL_URI` or `..._RELATIVE_URI`, with the optional authorization
    * token from `AWS_CONTAINER_AUTHORIZATION_TOKEN` or `..._TOKEN_FILE`. None outside a container.
    */
  val container: IO[AwsConfigError, Option[CredentialsProvider]] =
    def env(name: String) = System.env(name).mapError(e => AwsConfigError(s"reading $name: ${e.getMessage}")).map(_.filter(_.nonEmpty))
    for
      full     <- env("AWS_CONTAINER_CREDENTIALS_FULL_URI")
      relative <- env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
      token    <- env("AWS_CONTAINER_AUTHORIZATION_TOKEN")
      file     <- env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
      uri       = full.orElse(relative.map("http://169.254.170.2" + _))
      provider <- ZIO.foreach(uri)(u => refreshing(fetchContainer(u, token, file)))
    yield provider

  private val http = HttpClient.newHttpClient().nn

  private def fetchContainer(uri: String, token: Option[String], tokenFile: Option[String]): IO[AwsError, (AwsCredentials, Option[Instant])] =
    ZIO
      .attemptBlocking:
        val auth    = tokenFile.map(f => Files.readString(Paths.get(f), UTF_8).nn.trim.nn).orElse(token)
        val request = auth.foldLeft(HttpRequest.newBuilder(URI(uri)).nn.GET().nn)((b, t) => b.header("Authorization", t).nn).build().nn
        val response = http.send(request, HttpResponse.BodyHandlers.ofString()).nn
        (response.statusCode, response.body.nn)
      .mapError(AwsError.Transport(_))
      .flatMap:
        case (200, body) => ZIO.fromEither(parseContainer(body)).mapError(AwsError.Decode("container-credentials", _))
        case (code, body) => ZIO.fail(AwsError.Service("container-credentials", code, Some("CredentialsUnavailable"), Some(body.take(200))))

  def parseContainer(body: String): Either[String, (AwsCredentials, Option[Instant])] =
    def field(json: Json.Obj, name: String) = json.get(name).flatMap(_.asString).toRight(s"no $name in container credentials")
    for
      json   <- body.fromJson[Json.Obj]
      key    <- field(json, "AccessKeyId")
      secret <- field(json, "SecretAccessKey")
      token   = json.get("Token").flatMap(_.asString)
      expiry  = json.get("Expiration").flatMap(_.asString).flatMap(e => scala.util.Try(Instant.parse(e).nn).toOption)
    yield (AwsCredentials(AccessKeyId(key), SecretAccessKey(secret), token.map(SessionToken(_))), expiry)
