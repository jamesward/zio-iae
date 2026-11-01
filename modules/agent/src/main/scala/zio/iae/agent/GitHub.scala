package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.{KeyFactory, PrivateKey, Signature}
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Instant
import java.util.Base64
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** A pull request the agent opened. */
final case class PullRequest(number: Int, url: String) derives CanEqual

/** Where the agent pushes its branches and opens pull requests. Branches it pushes are always `iae/...`. */
trait PullRequests:
  /** The URL to push to, with whatever credentials that needs. */
  def pushUrl: IO[String, String]
  def open(branch: String, base: String, title: String, body: String): IO[String, PullRequest]

/** A GitHub repository as `owner/name`. */
final case class GitHubRepo(owner: String, name: String) derives CanEqual:
  def fullName: String = s"$owner/$name"

object GitHubRepo:
  /** From a clone URL: `https://github.com/owner/name(.git)` or `git@github.com:owner/name(.git)`. */
  def fromUrl(url: String): Either[String, GitHubRepo] =
    val path = url.stripSuffix("/").stripSuffix(".git").replaceFirst("^(https?://[^/]+/|git@[^:]+:)", "")
    path.split('/').toList match
      case owner :: name :: Nil if owner.nonEmpty && name.nonEmpty => Right(GitHubRepo(owner, name))
      case _                                                       => Left(s"'$url' is not a GitHub repository URL")

/** A GitHub App installation: app JWTs (RS256, from the app's private key) exchanged for installation tokens, which
  * are kept until five minutes before they expire.
  */
final case class GitHubApp(http: Client, api: URL, appId: String, key: PrivateKey, installation: String, cache: Ref.Synchronized[Option[(String, Instant)]]):
  def jwt(now: Instant): String = GitHubApp.jwt(appId, key, now)

  def token: IO[String, String] =
    cache.modifyZIO: cached =>
      Clock.ClockLive.instant.flatMap: now =>
        cached match
          case Some((t, expires)) if expires.minusSeconds(300).nn.isAfter(now) => ZIO.succeed((t, cached))
          case _ =>
            call(Method.POST, s"/app/installations/$installation/access_tokens", jwt(now), None).flatMap: json =>
              val parsed = for
                t <- json.get("token").flatMap(_.asString)
                e <- json.get("expires_at").flatMap(_.asString).flatMap(s => scala.util.Try(Instant.parse(s).nn).toOption)
              yield (t, e)
              ZIO.fromOption(parsed).orElseFail(s"GitHub returned no installation token: ${json.toJson.take(200)}").map(p => (p._1, Some(p)))

  def call(method: Method, path: String, bearer: String, body: Option[Json]): IO[String, Json.Obj] =
    val request = Request(
      method = method,
      url = api.addPath(path),
      headers = Headers(
        Header.Authorization.Bearer(bearer),
        Header.Accept(MediaType.application.json),
        Header.Custom("X-GitHub-Api-Version", "2022-11-28"),
        Header.Custom("User-Agent", "iae"),
      ),
      body = body.fold(Body.empty)(b => Body.fromString(b.toJson)),
    )
    for
      response <- http.batched(request).mapError(e => s"GitHub $path: ${e.getMessage}")
      text     <- response.body.asString.mapError(e => s"GitHub $path: ${e.getMessage}")
      _        <- ZIO.fail(s"GitHub $path answered ${response.status.code}: ${text.take(300)}").unless(response.status.isSuccess)
      json     <- ZIO.fromEither(text.fromJson[Json.Obj]).mapError(e => s"GitHub $path: $e")
    yield json

object GitHubApp:
  def make(http: Client, api: URL, appId: String, pem: String, installation: String): IO[String, GitHubApp] =
    for
      key   <- ZIO.fromEither(privateKey(pem))
      cache <- Ref.Synchronized.make(Option.empty[(String, Instant)])
    yield GitHubApp(http, api, appId, key, installation, cache)

  /** An app JWT: issued a minute ago (for clock drift), valid for nine minutes, as GitHub asks. */
  def jwt(appId: String, key: PrivateKey, now: Instant): String =
    val b64     = Base64.getUrlEncoder.nn.withoutPadding.nn
    val header  = b64.encodeToString("""{"alg":"RS256","typ":"JWT"}""".getBytes(UTF_8))
    val payload = b64.encodeToString(s"""{"iat":${now.getEpochSecond - 60},"exp":${now.getEpochSecond + 540},"iss":"$appId"}""".getBytes(UTF_8))
    val signer  = Signature.getInstance("SHA256withRSA").nn
    signer.initSign(key)
    signer.update(s"$header.$payload".getBytes(UTF_8))
    s"$header.$payload.${b64.encodeToString(signer.sign())}"

  /** The app's private key, as GitHub issues it (PKCS#1, `BEGIN RSA PRIVATE KEY`) or as PKCS#8. */
  def privateKey(pem: String): Either[String, PrivateKey] =
    val pkcs1 = pem.contains("BEGIN RSA PRIVATE KEY")
    val body  = pem.linesIterator.filterNot(_.startsWith("-----")).mkString.trim
    scala.util.Try:
      val der  = Base64.getDecoder.nn.decode(body).nn
      val spec = if pkcs1 then pkcs1ToPkcs8(der) else der
      KeyFactory.getInstance("RSA").nn.generatePrivate(PKCS8EncodedKeySpec(spec)).nn
    .toEither.left.map(e => s"the GitHub App private key can't be read: ${e.getMessage}")

  /** Wraps a PKCS#1 RSA key in the PKCS#8 structure Java reads. */
  private def pkcs1ToPkcs8(pkcs1: Array[Byte]): Array[Byte] =
    def length(n: Int): Array[Byte] =
      if n < 128 then Array(n.toByte)
      else
        val bytes = BigInt(n).toByteArray.dropWhile(_ == 0)
        (0x80 | bytes.length).toByte +: bytes
    def tlv(tag: Int, content: Array[Byte]): Array[Byte] = (tag.toByte +: length(content.length)) ++ content
    val version   = Array[Byte](0x02, 0x01, 0x00)
    val algorithm = Array[Byte](0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86.toByte, 0x48, 0x86.toByte, 0xf7.toByte, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00)
    tlv(0x30, version ++ algorithm ++ tlv(0x04, pkcs1))

/** Pull requests through a GitHub App installation on one repository. */
final case class GitHubPullRequests(app: GitHubApp, repo: GitHubRepo) extends PullRequests:
  def pushUrl: IO[String, String] = app.token.map(t => s"https://x-access-token:$t@github.com/${repo.fullName}.git")

  def open(branch: String, base: String, title: String, body: String): IO[String, PullRequest] =
    for
      token <- app.token
      json <- app.call(
                Method.POST,
                s"/repos/${repo.fullName}/pulls",
                token,
                Some(Json.Obj("title" -> Json.Str(title), "head" -> Json.Str(branch), "base" -> Json.Str(base), "body" -> Json.Str(body))),
              )
      pr <- ZIO
              .fromOption(json.get("number").flatMap(_.asNumber).map(_.value.intValue).zip(json.get("html_url").flatMap(_.asString)))
              .orElseFail("GitHub returned no pull request number")
    yield PullRequest(pr._1, pr._2)
