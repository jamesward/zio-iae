package zio.iae.aws.client

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import zio.http.URL
import zio.test.*

/** Vectors from the S3 SigV4 header-signing examples in the AWS documentation. */
object SigV4Spec extends ZIOSpecDefault:
  val credentials =
    AwsCredentials(AccessKeyId("AKIAIOSFODNN7EXAMPLE"), SecretAccessKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), None)
  val scope = SigV4.Scope(Region.UsEast1, "s3")
  val at    = Instant.parse("2013-05-24T00:00:00Z")

  def authorization(headers: List[(String, String)]): Option[String] = headers.collectFirst:
    case ("Authorization", value) => value

  def spec = suite("SigV4")(
    test("GET object"):
      val request = SigV4.Request(
        method = "GET",
        host = "examplebucket.s3.amazonaws.com",
        canonicalPath = "/test.txt",
        query = Nil,
        headers = List("range" -> "bytes=0-9", "x-amz-content-sha256" -> SigV4.emptyPayloadHash),
        payloadHash = SigV4.emptyPayloadHash,
      )
      val auth = authorization(SigV4.sign(request, credentials, scope, at))
      assertTrue(
        auth.exists(_.contains("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request")),
        auth.exists(_.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date")),
        auth.exists(_.endsWith("Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")),
      )
    ,
    test("PUT object with an encoded key and a payload"):
      val payloadHash = SigV4.sha256Hex("Welcome to Amazon S3.".getBytes(UTF_8))
      val request = SigV4.Request(
        method = "PUT",
        host = "examplebucket.s3.amazonaws.com",
        canonicalPath = "/" + SigV4.uriEncode("test$file.text", encodeSlash = false),
        query = Nil,
        headers = List(
          "date"                 -> "Fri, 24 May 2013 00:00:00 GMT",
          "x-amz-storage-class"  -> "REDUCED_REDUNDANCY",
          "x-amz-content-sha256" -> payloadHash,
        ),
        payloadHash = payloadHash,
      )
      val auth = authorization(SigV4.sign(request, credentials, scope, at))
      assertTrue(
        payloadHash == "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
        auth.exists(_.endsWith("Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")),
      )
    ,
    test("query strings are sorted and fully encoded"):
      assertTrue(
        SigV4.canonicalQuery(List("prefix" -> "a b/c", "list-type" -> "2")) == "list-type=2&prefix=a%20b%2Fc",
        SigV4.canonicalQuery(List("tagging" -> "")) == "tagging=",
      )
    ,
    // S3 signs the decoded key with its own encoding, so what matters is that the key arrives intact, even where
    // zio-http re-encodes a character differently (it sends `*` rather than `%2A`).
    test("zio-http delivers every key intact"):
      val keys = List("photos/a.jpg", "my file (1)!.jpg", "ümlaut/ß.txt", "a+b=c&d.txt", "tilde~star*.bin")
      val delivered = keys.map: key =>
        val url = "http://localhost:4566/bucket/" + SigV4.uriEncode(key, encodeSlash = false)
        URL.decode(url).map(u => java.net.URI(u.encode).getPath)
      assertTrue(delivered == keys.map(k => Right("/bucket/" + k)))
    ,
  )
