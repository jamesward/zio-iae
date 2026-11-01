package zio.iae.webhook

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

final case class WebhookSecret(value: String):
  override def toString: String = "WebhookSecret(***)"

/** Request signatures of GitHub (`X-Hub-Signature-256`) and Slack (`X-Slack-Signature`), compared in constant time. */
object Signatures:
  def github(secret: WebhookSecret, body: Array[Byte]): String = "sha256=" + hmacHex(secret, body)

  def slack(secret: WebhookSecret, timestamp: String, body: Array[Byte]): String =
    "v0=" + hmacHex(secret, s"v0:$timestamp:".getBytes(UTF_8) ++ body)

  def matches(expected: String, actual: Option[String]): Boolean =
    actual.exists(a => MessageDigest.isEqual(expected.getBytes(UTF_8), a.getBytes(UTF_8)))

  private def hmacHex(secret: WebhookSecret, data: Array[Byte]): String =
    val mac = Mac.getInstance("HmacSHA256").nn
    mac.init(SecretKeySpec(secret.value.getBytes(UTF_8), "HmacSHA256"))
    HexFormat.of().nn.formatHex(mac.doFinal(data)).nn
