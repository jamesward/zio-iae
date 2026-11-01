package zio.iae.aws.client

import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.test.*

/** Pins down how request bodies are written, since AWS JSON APIs expect absent optional fields to be left out. */
object JsonEncodingSpec extends ZIOSpecDefault:
  final case class Request(@fieldName("Name") name: String, @fieldName("Token") token: Option[String]) derives Schema

  def spec = suite("AWS JSON request encoding")(
    test("an absent optional field is left out, not written as null"):
      assertTrue(
        AwsJsonClient.encode(Request("a", None)) == """{"Name":"a"}""",
        AwsJsonClient.encode(Request("a", Some("t"))) == """{"Name":"a","Token":"t"}""",
      )
  )
