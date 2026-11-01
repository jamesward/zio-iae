package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import scala.xml.Elem
import zio.*
import zio.http.{Client, Method}

/** A Query-protocol service (form-encoded `Action`, XML responses): IAM, STS, SNS. */
final case class AwsQueryProtocol(service: AwsService, version: String)

final case class AwsQueryClient(http: Client, config: AwsConfig):
  def call(protocol: AwsQueryProtocol, action: String, params: List[(String, String)]): IO[AwsError, Elem] =
    val serviceName = protocol.service.signingName
    val form = (("Action" -> action) :: ("Version" -> protocol.version) :: params)
      .map((k, v) => s"${SigV4.uriEncode(k, encodeSlash = true)}=${SigV4.uriEncode(v, encodeSlash = true)}")
      .mkString("&")
    val headers = List("content-type" -> "application/x-www-form-urlencoded; charset=utf-8")
    val attempt =
      for
        req <- AwsHttp
                 .request(config, protocol.service, Method.POST, AwsHttp.root(config, protocol.service), headers, Chunk.fromArray(form.getBytes(UTF_8)), contentSha256Header = false)
                 .mapError(AwsError.InvalidRequest(_))
        resp <- AwsHttp.send(http, req).mapError(AwsError.Transport(_))
        text <- resp.body.asString.mapError(AwsError.Transport(_))
        elem <- ZIO.fromEither(Xml.parse(text)).mapError(AwsError.Decode(serviceName, _))
        _ <- ZIO.unless(resp.status.isSuccess):
               val error = elem \\ "Error"
               ZIO.fail(AwsError.Service(serviceName, resp.status.code, (error \ "Code").headOption.map(_.text), (error \ "Message").headOption.map(_.text)))
      yield elem
    attempt.retry(AwsHttp.retryPolicy && Schedule.recurWhile[AwsError](_.retryable))

object AwsQueryClient:
  val layer: ZLayer[Client & AwsConfig, Nothing, AwsQueryClient] = ZLayer.fromFunction(AwsQueryClient.apply)
