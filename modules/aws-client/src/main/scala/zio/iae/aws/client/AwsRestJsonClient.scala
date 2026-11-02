package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.{Client, Method}
import zio.json.*
import zio.json.ast.Json

/** A REST-JSON service (the operation is the method and path, the body JSON): Security Hub, GuardDuty, Inspector,
  * IAM Access Analyzer. Requests and responses are JSON values, read by the clients that know their shapes.
  */
final case class AwsRestJsonClient(http: Client, config: AwsConfig):
  def call(service: AwsService, method: Method, path: String, body: Option[Json] = None, query: List[(String, String)] = Nil): IO[AwsError, Json] =
    val bytes   = Chunk.fromArray(body.fold("")(_.toJson).getBytes(UTF_8))
    val headers = List("content-type" -> "application/json")
    val target  = AwsHttp.root(config, service).copy(path = path, query = query)
    val attempt =
      for
        req  <- AwsHttp.request(config, service, method, target, headers, bytes, contentSha256Header = false, doubleEncodePath = true).mapError(AwsError.InvalidRequest(_))
        resp <- AwsHttp.send(http, req).mapError(AwsError.Transport(_))
        text <- resp.body.asString.mapError(AwsError.Transport(_))
        _ <- ZIO.unless(resp.status.isSuccess):
               ZIO.fail(AwsJsonClient.errorOf(service.signingName, resp.status.code, resp.rawHeader("x-amzn-errortype"), text))
        json <- ZIO.fromEither(if text.isBlank then Right(Json.Obj()) else text.fromJson[Json]).mapError(AwsError.Decode(service.signingName, _))
      yield json
    attempt.retry(AwsHttp.retryPolicy && Schedule.recurWhile[AwsError](_.retryable))

object AwsRestJsonClient:
  val layer: ZLayer[Client & AwsConfig, Nothing, AwsRestJsonClient] = ZLayer.fromFunction(AwsRestJsonClient.apply)

  /** Reading JSON values the services return. */
  extension (json: Json)
    def field(name: String): Option[Json]     = json.asObject.flatMap(_.get(name))
    def str(name: String): Option[String]     = field(name).flatMap(_.asString)
    def num(name: String): Option[Double]     = field(name).flatMap(_.asNumber).map(_.value.doubleValue)
    def bool(name: String): Option[Boolean]   = field(name).flatMap(_.asBoolean)
    def items(name: String): List[Json]       = field(name).flatMap(_.asArray).fold(Nil)(_.toList)
    def path(names: String*): Option[Json]    = names.foldLeft(Option(json))((j, n) => j.flatMap(_.field(n)))
