package zio.iae.aws.client

import java.time.Instant
import zio.*
import zio.test.*

object CredentialsSpec extends ZIOSpecDefault:
  val creds = AwsCredentials(AccessKeyId("AKID"), SecretAccessKey("secret"), Some(SessionToken("token")))

  def spec = suite("Credentials")(
    test("ECS container credentials parse, with the token and expiry"):
      val body = """{"AccessKeyId":"AKID","SecretAccessKey":"secret","Token":"token","Expiration":"2026-11-01T12:00:00Z","RoleArn":"x"}"""
      assertTrue(
        CredentialsProvider.parseContainer(body).exists: (c, expiry) =>
          c.toString == creds.toString && c.sessionToken.map(_.value).contains("token") &&
            expiry.exists(_.toString == "2026-11-01T12:00:00Z"),
        CredentialsProvider.parseContainer("""{"AccessKeyId":"AKID"}""").left.exists(_.contains("SecretAccessKey")),
      ),
    test("refreshing credentials are kept until five minutes before they expire"):
      for
        fetches  <- Ref.make(0)
        expiry   <- Ref.make(Instant.now.nn.plusSeconds(3600).nn)
        provider <- CredentialsProvider.refreshing(fetches.update(_ + 1) *> expiry.get.map(e => (creds, Some(e))))
        _        <- provider.credentials.repeatN(4)
        cached   <- fetches.get
        _        <- expiry.set(Instant.now.nn.plusSeconds(120).nn)
        _        <- provider.credentials // still the cached ones, which expire in an hour
        _        <- provider.credentials.repeatN(2)
        stale    <- fetches.get
      yield assertTrue(cached == 1, stale == 1),
    test("credentials close to expiry are fetched again, and failures aren't cached"):
      for
        fetches  <- Ref.make(0)
        fail     <- Ref.make(true)
        provider <- CredentialsProvider.refreshing:
                      fetches.update(_ + 1) *> fail.get.flatMap: f =>
                        if f then ZIO.fail(AwsError.InvalidRequest("no")) else ZIO.succeed((creds, Some(Instant.now.nn.plusSeconds(60).nn)))
        failed   <- provider.credentials.either
        _        <- fail.set(false)
        _        <- provider.credentials.repeatN(2)
        count    <- fetches.get
      yield assertTrue(failed.isLeft, count == 4),
  )
