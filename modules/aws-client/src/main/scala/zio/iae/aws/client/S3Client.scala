package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.{Instant, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Base64
import scala.util.Try
import scala.xml.Node
import zio.*
import zio.http.{Client, Method, Request, Response}
import zio.stream.ZStream

/** A thin S3 client: only the operations zio-iae needs, signed with SigV4, with typed errors and retries. */
final case class S3Client(http: Client, config: AwsConfig):
  import S3Client.*

  def createBucket(bucket: BucketName): IO[S3Error, Unit] =
    val body =
      if config.region == Region.UsEast1 then Chunk.empty
      else
        utf8(
          s"""<CreateBucketConfiguration xmlns="$xmlns"><LocationConstraint>${config.region.value}</LocationConstraint></CreateBucketConfiguration>"""
        )
    execute(Call(Method.PUT, bucket, body = body)).unit

  def bucketExists(bucket: BucketName): IO[S3Error, Boolean] =
    execute(Call(Method.HEAD, bucket)).as(true).catchSome:
      case S3Error.NoSuchBucket(_) | S3Error.Service(404, _, _) => ZIO.succeed(false)

  def deleteBucket(bucket: BucketName): IO[S3Error, Unit] =
    execute(Call(Method.DELETE, bucket)).unit

  def getBucketVersioning(bucket: BucketName): IO[S3Error, VersioningStatus] =
    executeXml(Call(Method.GET, bucket, query = List("versioning" -> ""))): elem =>
      Xml.child(elem, "Status") match
        case None | Some("")   => Right(VersioningStatus.Off)
        case Some("Enabled")   => Right(VersioningStatus.Enabled)
        case Some("Suspended") => Right(VersioningStatus.Suspended)
        case Some(other)       => Left(s"unknown versioning status '$other'")

  def putBucketVersioning(bucket: BucketName, enabled: Boolean): IO[S3Error, Unit] =
    val status = if enabled then "Enabled" else "Suspended"
    val body   = utf8(s"""<VersioningConfiguration xmlns="$xmlns"><Status>$status</Status></VersioningConfiguration>""")
    execute(Call(Method.PUT, bucket, query = List("versioning" -> ""), body = body, md5 = true)).unit

  def getBucketTagging(bucket: BucketName): IO[S3Error, Map[String, String]] =
    executeXml(Call(Method.GET, bucket, query = List("tagging" -> ""))): elem =>
      Right((elem \ "TagSet" \ "Tag").map(tag => (tag \ "Key").text -> (tag \ "Value").text).toMap)
    .catchSome:
      case S3Error.Service(_, Some("NoSuchTagSet"), _) => ZIO.succeed(Map.empty)

  def putBucketTagging(bucket: BucketName, tags: Map[String, String]): IO[S3Error, Unit] =
    val tagXml = tags.toList.sorted
      .map((k, v) => s"<Tag><Key>${Xml.escape(k)}</Key><Value>${Xml.escape(v)}</Value></Tag>")
      .mkString
    val body = utf8(s"""<Tagging xmlns="$xmlns"><TagSet>$tagXml</TagSet></Tagging>""")
    execute(Call(Method.PUT, bucket, query = List("tagging" -> ""), body = body, md5 = true)).unit

  def putObject(bucket: BucketName, key: S3Key, body: Chunk[Byte], contentType: Option[String]): IO[S3Error, Unit] =
    val headers = contentType.map("content-type" -> _).toList
    execute(Call(Method.PUT, bucket, Some(key), body = body, headers = headers)).unit

  def getObject(bucket: BucketName, key: S3Key): ZStream[Any, S3Error, Byte] =
    val call = Call(Method.GET, bucket, Some(key))
    ZStream
      .fromZIO(request(call))
      .flatMap: req =>
        http.stream(req): resp =>
          if resp.status.isSuccess then resp.body.asStream
          else ZStream.fromZIO(failure(resp, call))
      .mapError:
        case e: S3Error   => e
        case t: Throwable => S3Error.Transport(t)

  def headObject(bucket: BucketName, key: S3Key): IO[S3Error, Option[S3Object]] =
    execute(Call(Method.HEAD, bucket, Some(key)))
      .flatMap: resp =>
        val size     = resp.rawHeader("content-length").flatMap(_.toLongOption)
        val eTag     = resp.rawHeader("etag").map(unquote)
        val modified = resp.rawHeader("last-modified").flatMap(parseHttpDate)
        ZIO
          .fromOption(size.zip(eTag))
          .mapBoth(_ => S3Error.Decode("HEAD response lacks Content-Length or ETag"), (s, e) => Some(S3Object(key, s, e, modified)))
      .catchSome:
        case S3Error.NoSuchKey(_, _) | S3Error.Service(404, _, _) => ZIO.none

  def deleteObject(bucket: BucketName, key: S3Key): IO[S3Error, Unit] =
    execute(Call(Method.DELETE, bucket, Some(key))).unit

  def listObjects(bucket: BucketName, prefix: Option[String]): ZStream[Any, S3Error, S3Object] =
    ZStream.paginateChunkZIO(Option.empty[String]): token =>
      val query = List("list-type" -> "2") ++ prefix.map("prefix" -> _) ++ token.map("continuation-token" -> _)
      executeXml(Call(Method.GET, bucket, query = query))(parseListPage).map(page => (page.objects, page.next.map(Some(_))))

  private def executeXml[A](call: Call)(decode: scala.xml.Elem => Either[String, A]): IO[S3Error, A] =
    execute(call)
      .flatMap(_.body.asString.mapError(S3Error.Transport(_)))
      .flatMap(text => ZIO.fromEither(Xml.parse(text).flatMap(decode)).mapError(S3Error.Decode(_)))

  private def execute(call: Call): IO[S3Error, Response] =
    request(call)
      .flatMap(req => AwsHttp.send(http, req).mapError(S3Error.Transport(_)))
      .flatMap(resp => if resp.status.isSuccess then ZIO.succeed(resp) else failure(resp, call))
      .retry(retryPolicy)

  private def request(call: Call): IO[S3Error, Request] =
    val md5Header = Option.when(call.md5)("content-md5" -> Base64.getEncoder.nn.encodeToString(md5(call.body.toArray)).nn)
    AwsHttp
      .request(config, service, call.method, targetOf(call), call.headers ++ md5Header.toList, call.body, contentSha256Header = true)
      .mapError(S3Error.InvalidRequest(_))

  private def targetOf(call: Call): AwsHttp.Target =
    val encodedKey = call.key.fold("")(k => SigV4.uriEncode(k.value, encodeSlash = false))
    config.endpoint match
      case Some(endpoint) =>
        val keyPart = if encodedKey.isEmpty then "" else s"/$encodedKey"
        AwsHttp.Target(endpoint.scheme.value, endpoint.authority, s"/${call.bucket.value}$keyPart", call.query)
      case None =>
        AwsHttp.Target("https", s"${call.bucket.value}.s3.${config.region.value}.amazonaws.com", s"/$encodedKey", call.query)

  private def failure(resp: Response, call: Call): IO[S3Error, Nothing] =
    resp.body.asString.mapError(S3Error.Transport(_)).flatMap(text => ZIO.fail(errorOf(resp.status.code, text, call)))

object S3Client:
  val layer: ZLayer[Client & AwsConfig, Nothing, S3Client] = ZLayer.fromFunction(S3Client.apply)

  private val xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

  private val service = AwsService("s3", "s3")

  private val retryPolicy = AwsHttp.retryPolicy && Schedule.recurWhile[S3Error](_.retryable)

  private final case class Call(
      method: Method,
      bucket: BucketName,
      key: Option[S3Key] = None,
      query: List[(String, String)] = Nil,
      body: Chunk[Byte] = Chunk.empty,
      headers: List[(String, String)] = Nil,
      md5: Boolean = false,
  )

  private final case class ListPage(objects: Chunk[S3Object], next: Option[String])

  private def errorOf(status: Int, text: String, call: Call): S3Error =
    Xml.parse(text) match
      case Right(elem) =>
        val message = Xml.child(elem, "Message")
        Xml.child(elem, "Code") match
          case Some("NoSuchBucket")            => S3Error.NoSuchBucket(call.bucket)
          case Some("NoSuchKey")               => call.key.fold(S3Error.Service(status, Some("NoSuchKey"), message))(S3Error.NoSuchKey(call.bucket, _))
          case Some("BucketAlreadyOwnedByYou") => S3Error.BucketAlreadyOwnedByYou(call.bucket)
          case Some("BucketNotEmpty")          => S3Error.BucketNotEmpty(call.bucket)
          case Some("AccessDenied")            => S3Error.AccessDenied(message.getOrElse(call.bucket.value))
          case code                            => S3Error.Service(status, code, message)
      case Left(_) => S3Error.Service(status, None, Option.when(text.nonEmpty)(text.take(500)))

  private def parseListPage(elem: scala.xml.Elem): Either[String, ListPage] =
    val (errors, objects) = (elem \ "Contents").toList.partitionMap(parseObject)
    errors.headOption.toLeft:
      val truncated = Xml.child(elem, "IsTruncated").contains("true")
      ListPage(Chunk.fromIterable(objects), Xml.child(elem, "NextContinuationToken").filter(_ => truncated))

  private def parseObject(node: Node): Either[String, S3Object] =
    for
      key  <- S3Key.from((node \ "Key").text)
      size <- (node \ "Size").text.toLongOption.toRight(s"invalid Size for ${key.value}")
    yield S3Object(key, size, unquote((node \ "ETag").text), Try(Instant.parse((node \ "LastModified").text).nn).toOption)

  private def parseHttpDate(value: String): Option[Instant] =
    Try(ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).nn.toInstant.nn).toOption

  private def unquote(value: String): String = value.stripPrefix("\"").stripSuffix("\"")

  private def utf8(text: String): Chunk[Byte] = Chunk.fromArray(text.getBytes(UTF_8))

  private def md5(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("MD5").nn.digest(bytes).nn
