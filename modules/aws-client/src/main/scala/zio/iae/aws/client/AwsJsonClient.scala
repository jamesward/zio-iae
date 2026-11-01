package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.{Client, Method}
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.schema.codec.JsonCodec

/** A JSON-protocol service (`X-Amz-Target` operations): SQS, DynamoDB, Secrets Manager, Resource Groups Tagging. */
final case class AwsJsonProtocol(service: AwsService, targetPrefix: String, jsonVersion: String)

final case class AwsJsonClient(http: Client, config: AwsConfig):
  import AwsJsonClient.*

  def call[Req: Schema, Res: Schema](protocol: AwsJsonProtocol, operation: String, request: Req): IO[AwsError, Res] =
    val serviceName = protocol.service.signingName
    val body        = Chunk.fromArray(JsonCodec.jsonEncoder(Schema[Req]).encodeJson(request, None).toString.getBytes(UTF_8))
    val headers = List(
      "content-type" -> s"application/x-amz-json-${protocol.jsonVersion}",
      "x-amz-target" -> s"${protocol.targetPrefix}.$operation",
    )
    val attempt =
      for
        req  <- AwsHttp.request(config, protocol.service, Method.POST, AwsHttp.root(config, protocol.service), headers, body, contentSha256Header = false)
                  .mapError(AwsError.InvalidRequest(_))
        resp <- AwsHttp.send(http, req).mapError(AwsError.Transport(_))
        text <- resp.body.asString.mapError(AwsError.Transport(_))
        _ <- ZIO.unless(resp.status.isSuccess):
               ZIO.fail(errorOf(serviceName, resp.status.code, resp.rawHeader("x-amzn-errortype"), text))
        decoded <- ZIO
                     .fromEither(JsonCodec.jsonDecoder(Schema[Res]).decodeJson(if text.isBlank then "{}" else text))
                     .mapError(AwsError.Decode(serviceName, _))
      yield decoded
    attempt.retry(AwsHttp.retryPolicy && Schedule.recurWhile[AwsError](_.retryable))

object AwsJsonClient:
  val layer: ZLayer[Client & AwsConfig, Nothing, AwsJsonClient] = ZLayer.fromFunction(AwsJsonClient.apply)

  /** Responses of operations that return nothing useful. */
  final case class Empty() derives Schema

  private final case class ErrorBody(@fieldName("__type") tpe: Option[String], message: Option[String], @fieldName("Message") upperMessage: Option[String])
      derives Schema

  private def errorOf(service: String, status: Int, header: Option[String], text: String): AwsError =
    val parsed = JsonCodec.jsonDecoder(Schema[ErrorBody]).decodeJson(text).toOption
    val code   = header.orElse(parsed.flatMap(_.tpe)).map(_.split('#').last.split(':').head)
    AwsError.Service(service, status, code, parsed.flatMap(p => p.message.orElse(p.upperMessage)).orElse(Option.when(text.nonEmpty)(text.take(300))))
