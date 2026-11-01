package zio.iae.aws.client

import zio.*
import zio.http.{Body, Client, Header, Headers, Method, Request, Response, URL}

/** How to reach and sign for one AWS service. `global` services (IAM) have one endpoint and sign for us-east-1. */
final case class AwsService(signingName: String, endpointPrefix: String, global: Boolean = false)

/** A failure from a non-S3 AWS service. */
enum AwsError derives CanEqual:
  case Service(service: String, status: Int, code: Option[String], detail: Option[String])
  case Decode(service: String, detail: String)
  case InvalidRequest(detail: String)
  case Transport(cause: Throwable)

  def errorCode: Option[String] = this match
    case Service(_, _, code, _) => code
    case _                      => None

  def message: String = this match
    case Service(service, status, code, detail) => s"$service returned $status ${code.getOrElse("")}: ${detail.getOrElse("")}".trim
    case Decode(service, detail)               => s"could not read $service response: $detail"
    case InvalidRequest(detail)                => s"could not build request: $detail"
    case Transport(cause)                      => s"request failed: ${cause.getMessage}"

  def retryable: Boolean = this match
    case Service(_, status, code, _) =>
      status >= 500 || code.exists(c => Set("Throttling", "ThrottlingException", "RequestLimitExceeded", "TooManyRequestsException")(c.split('#').last))
    case Transport(_) => true
    case _            => false

/** Request building and signing shared by every client. */
private[client] object AwsHttp:
  final case class Target(scheme: String, authority: String, path: String, query: List[(String, String)]):
    def url: String =
      val q = SigV4.canonicalQuery(query)
      s"$scheme://$authority$path${if q.isEmpty then "" else s"?$q"}"

  /** The service's root, or the configured endpoint override (Floci) for every service. */
  def root(config: AwsConfig, service: AwsService): Target =
    config.endpoint match
      case Some(endpoint) => Target(endpoint.scheme.value, endpoint.authority, "/", Nil)
      case None =>
        val host = if service.global then s"${service.endpointPrefix}.amazonaws.com" else s"${service.endpointPrefix}.${config.region.value}.amazonaws.com"
        Target("https", host, "/", Nil)

  def signingRegion(config: AwsConfig, service: AwsService): Region =
    if service.global then Region.UsEast1 else config.region

  def request(
      config: AwsConfig,
      service: AwsService,
      method: Method,
      target: Target,
      headers: List[(String, String)],
      body: Chunk[Byte],
      contentSha256Header: Boolean,
  ): IO[String, Request] =
    val payloadHash = SigV4.sha256Hex(body.toArray)
    val all         = if contentSha256Header then ("x-amz-content-sha256" -> payloadHash) :: headers else headers
    val signable    = SigV4.Request(method.name, target.authority, target.path, target.query, all, payloadHash)
    for
      now <- Clock.ClockLive.instant // AWS checks the signing time against its own clock
      url <- ZIO.fromEither(URL.decode(target.url)).mapError(e => s"${target.url}: ${e.getMessage}")
      signed = SigV4.sign(signable, config.credentials, SigV4.Scope(signingRegion(config, service), service.signingName), now)
      sent   = ("host" -> target.authority) :: all ++ signed
    yield Request(method = method, url = url, headers = Headers(sent.map(Header.Custom(_, _))), body = Body.fromChunk(body))

  def send(http: Client, request: Request): IO[Throwable, Response] = http.batched(request)

  val retryPolicy: Schedule[Any, Any, Any] = Schedule.exponential(100.millis).jittered && Schedule.recurs(4)
