package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.{KeyPairGenerator, Signature}
import java.util.Base64
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json
import zio.test.*

object GitHubSpec extends ZIOSpecDefault:
  val keys = { val g = KeyPairGenerator.getInstance("RSA").nn; g.initialize(2048); g.generateKeyPair().nn }

  def pem(kind: String, der: Array[Byte]): String =
    s"-----BEGIN $kind-----\n${Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).nn.encodeToString(der)}\n-----END $kind-----\n"

  val pkcs8 = keys.getPrivate.nn.getEncoded.nn
  // Java's PKCS#8 for a 2048-bit RSA key: SEQUENCE(4) INTEGER 0(3) AlgorithmIdentifier(15) OCTET STRING(4) <PKCS#1>
  val pkcs1 = pkcs8.drop(26)

  def verifies(jwt: String): Boolean =
    val parts  = jwt.split('.')
    val verify = Signature.getInstance("SHA256withRSA").nn
    verify.initVerify(keys.getPublic)
    verify.update(s"${parts(0)}.${parts(1)}".getBytes(UTF_8))
    verify.verify(Base64.getUrlDecoder.nn.decode(parts(2)))

  /** A GitHub API that issues installation tokens to valid app JWTs and opens pull requests for those tokens. */
  def fakeGitHub(tokens: Ref[Int], pulls: Ref[Chunk[Json.Obj]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / "app" / "installations" / string("id") / "access_tokens" -> handler: (id: String, req: Request) =>
        val jwt = req.header(Header.Authorization).collect { case Header.Authorization.Bearer(t) => t.stringValue }.getOrElse("")
        if id != "42" || !verifies(jwt) then ZIO.succeed(Response.status(Status.Unauthorized))
        else tokens.update(_ + 1).as(Response.json("""{"token":"ghs_secret","expires_at":"2099-01-01T00:00:00Z"}""")),
      Method.POST / "repos" / string("owner") / string("repo") / "pulls" -> handler: (owner: String, repo: String, req: Request) =>
        for
          body <- req.body.asString.orElseSucceed("")
          ok    = req.header(Header.Authorization).exists { case Header.Authorization.Bearer(t) => t.stringValue == "ghs_secret"; case _ => false }
          json  = body.fromJson[Json.Obj].getOrElse(Json.Obj())
          _    <- pulls.update(_ :+ json.add("repo", Json.Str(s"$owner/$repo"))).when(ok)
        yield if ok then Response.json(s"""{"number":7,"html_url":"https://github.com/$owner/$repo/pull/7"}""") else Response.status(Status.Unauthorized),
    )

  def spec = suite("GitHub")(
    test("repository URLs give owner and name"):
      assertTrue(
        GitHubRepo.fromUrl("https://github.com/jamesward/zio-iae.git") == Right(GitHubRepo("jamesward", "zio-iae")),
        GitHubRepo.fromUrl("git@github.com:jamesward/zio-iae.git") == Right(GitHubRepo("jamesward", "zio-iae")),
        GitHubRepo.fromUrl("https://example.com/x").isLeft,
      ),
    test("the app's key reads as PKCS#1 (as GitHub issues it) or PKCS#8, and app JWTs are signed with it"):
      val now = java.time.Instant.parse("2026-11-01T00:00:00Z").nn
      val fromPkcs1 = GitHubApp.privateKey(pem("RSA PRIVATE KEY", pkcs1))
      val fromPkcs8 = GitHubApp.privateKey(pem("PRIVATE KEY", pkcs8))
      val jwt       = GitHubApp.jwt("123", fromPkcs1.toOption.get, now)
      val claims    = String(Base64.getUrlDecoder.nn.decode(jwt.split('.')(1)), UTF_8)
      assertTrue(
        fromPkcs1.isRight,
        fromPkcs8.isRight,
        verifies(jwt),
        claims.contains("\"iss\":\"123\""),
        claims.contains(s"\"iat\":${now.getEpochSecond - 60}"),
        GitHubApp.privateKey("nonsense").isLeft,
      ),
    test("an installation token is fetched with an app JWT, kept, and used to open a pull request"):
      ZIO.scoped:
        for
          tokens <- Ref.make(0)
          pulls  <- Ref.make(Chunk.empty[Json.Obj])
          server <- Server.defaultWithPort(0).build.orDie
          port   <- Server.install(fakeGitHub(tokens, pulls)).provideEnvironment(server)
          http   <- ZIO.service[Client]
          app    <- GitHubApp.make(http, URL.decode(s"http://localhost:$port").toOption.get, "123", pem("RSA PRIVATE KEY", pkcs1), "42")
          prs     = GitHubPullRequests(app, GitHubRepo("acme", "infra"))
          url    <- prs.pushUrl
          pr     <- prs.open("iae/fix-abc", "main", "Fix the manifest", "Because.")
          count  <- tokens.get
          opened <- pulls.get
        yield assertTrue(
          url == "https://x-access-token:ghs_secret@github.com/acme/infra.git",
          pr == PullRequest(7, "https://github.com/acme/infra/pull/7"),
          count == 1,
          opened.head.get("head").contains(Json.Str("iae/fix-abc")),
          opened.head.get("base").contains(Json.Str("main")),
          opened.head.get("repo").contains(Json.Str("acme/infra")),
        ),
    test("tokens are kept out of push errors, and fixes can't write outside the repository or into .git"):
      val dir = java.nio.file.Paths.get("/work/repo").nn
      assertTrue(
        FixWorkspace.redact("git push https://x-access-token:ghs_secret@github.com/a/b.git failed", "https://x-access-token:ghs_secret@github.com/a/b.git") ==
          "git push https://x-access-token:***@github.com/a/b.git failed",
        FixWorkspace.resolve(dir, "src/Main.scala").isRight,
        FixWorkspace.resolve(dir, "../etc/passwd").isLeft,
        FixWorkspace.resolve(dir, ".git/config").isLeft,
        FixWorkspace.resolve(dir, "/.git/hooks/pre-commit").isLeft,
      ),
  ).provide(Client.default.orDie) @@ TestAspect.withLiveClock
