package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** AWS Signature Version 4 request signing (header-based). Pure: the caller supplies the time. */
object SigV4:
  private val algorithm  = "AWS4-HMAC-SHA256"
  private val amzDateFmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").nn.withZone(ZoneOffset.UTC).nn
  private val dateFmt    = DateTimeFormatter.ofPattern("yyyyMMdd").nn.withZone(ZoneOffset.UTC).nn
  private val hex        = HexFormat.of().nn

  val emptyPayloadHash: String = sha256Hex(Array.emptyByteArray)

  /** What gets signed. `canonicalPath` must already be URI-encoded exactly as it is sent; `query` is decoded. */
  final case class Request(
      method: String,
      host: String,
      canonicalPath: String,
      query: List[(String, String)],
      headers: List[(String, String)],
      payloadHash: String,
  )

  final case class Scope(region: Region, service: String)

  /** The headers to add to the request: `x-amz-date`, the session token if any, and `Authorization`. */
  def sign(request: Request, credentials: AwsCredentials, scope: Scope, at: Instant): List[(String, String)] =
    val amzDate = amzDateFmt.format(at).nn
    val date    = dateFmt.format(at).nn
    val added =
      List("x-amz-date" -> amzDate) ++ credentials.sessionToken.map(t => "x-amz-security-token" -> t.value).toList
    val headers = (("host" -> request.host) :: request.headers ++ added)
      .map((k, v) => k.toLowerCase.nn -> v.trim.nn.replaceAll(" +", " ").nn)
      .sortBy(_._1)
    val signedHeaders    = headers.map(_._1).mkString(";")
    val canonicalHeaders = headers.map((k, v) => s"$k:$v\n").mkString
    val canonicalRequest = List(
      request.method,
      request.canonicalPath,
      canonicalQuery(request.query),
      canonicalHeaders,
      signedHeaders,
      request.payloadHash,
    ).mkString("\n")
    val credentialScope = s"$date/${scope.region.value}/${scope.service}/aws4_request"
    val stringToSign    = List(algorithm, amzDate, credentialScope, sha256Hex(canonicalRequest.getBytes(UTF_8))).mkString("\n")
    val signingKey = List(date, scope.region.value, scope.service, "aws4_request")
      .foldLeft(s"AWS4${credentials.secretAccessKey.value}".getBytes(UTF_8))(hmac)
    val signature = hex.formatHex(hmac(signingKey, stringToSign)).nn
    val authorization =
      s"$algorithm Credential=${credentials.accessKeyId.value}/$credentialScope, SignedHeaders=$signedHeaders, Signature=$signature"
    added :+ ("Authorization" -> authorization)

  def canonicalQuery(query: List[(String, String)]): String =
    query.map((k, v) => uriEncode(k, encodeSlash = true) -> uriEncode(v, encodeSlash = true)).sorted.map((k, v) => s"$k=$v").mkString("&")

  /** RFC 3986 encoding as SigV4 requires: unreserved characters stay, everything else is %XX of its UTF-8 bytes. */
  def uriEncode(value: String, encodeSlash: Boolean): String =
    val out = StringBuilder()
    value.getBytes(UTF_8).foreach: b =>
      val c = (b & 0xff).toChar
      if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || "-_.~".contains(c) then out += c
      else if c == '/' && !encodeSlash then out += c
      else out ++= f"%%${b & 0xff}%02X"
    out.result()

  def sha256Hex(bytes: Array[Byte]): String = hex.formatHex(MessageDigest.getInstance("SHA-256").nn.digest(bytes)).nn

  private def hmac(key: Array[Byte], data: String): Array[Byte] =
    val mac = Mac.getInstance("HmacSHA256").nn
    mac.init(SecretKeySpec(key, "HmacSHA256"))
    mac.doFinal(data.getBytes(UTF_8)).nn
