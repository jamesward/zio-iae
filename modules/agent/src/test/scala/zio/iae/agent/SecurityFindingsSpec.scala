package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.{AccessKeyId, AwsConfig, AwsCredentials, AwsRestJsonClient, Endpoint, InspectorClient, Region, SecretAccessKey}
import zio.iae.engine.*
import zio.iae.engine.policy.SlackGroup
import zio.iae.testkit.SecurityFake
import zio.test.*

/** The security auditor's findings: a threat is escalated to the owners at once, the rest reported once as a set,
  * with Inspector's findings on the images the env runs.
  */
object SecurityFindingsSpec extends ZIOSpecDefault:
  val app     = AppName("photo-app")
  val env     = EnvName("prod")
  val channel = "C_SECURITY"
  val web     = Address(app, env, ResourceKind.Service, LogicalId("web"))
  val uploads = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
  val role    = Address(app, env, ResourceKind.Role, Manifest.runtimeRole)

  object AnyOrder extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] = ZIO.succeed(true)

  /** A service that runs `photo-app:c7` and would roll back to `c5`. */
  object Running extends ResourceDriver:
    val kind = ResourceKind.Service
    def plan(address: Address, desired: ResourceSpec)              = ZIO.none
    def apply(change: Change[ResourceSpec], context: ApplyContext) = ZIO.unit
    def lastCommit(address: Address)                               = ZIO.none
    override def artifacts(address: Address)                       = ZIO.succeed(List("123.dkr.ecr.us-east-1.amazonaws.com/iae/photo-app:c7", "123.dkr.ecr.us-east-1.amazonaws.com/iae/photo-app:c5"))

  val found: Findings = (_, _) =>
    ZIO.succeed(List(
      SecurityFinding("GuardDuty", "gd-1", Severity.High, "Credentials used from outside AWS", Some(role), "The task role's credentials were used from 203.0.113.9.", threat = true),
      SecurityFinding("iae", "iae/uploads/public", Severity.High, "public access isn't blocked", Some(uploads), "", threat = false),
      SecurityFinding("Security Hub", "hub-low", Severity.Low, "Something minor", Some(uploads), "", threat = false),
    ))

  def spec = suite("Security findings")(
    test("a threat is escalated once to the owners; the other findings, and the images' vulnerabilities, are reported once"):
      ZIO.scoped:
        for
          repo    <- TestRepo.make
          ws       = repo.workspace(env)
          fake    <- SecurityFake.make.orDie
          http    <- ZIO.service[Client]
          config   = AwsConfig.static(Region.UsEast1, AwsCredentials(AccessKeyId("k"), SecretAccessKey("s"), None), Endpoint.parse(fake.url.encode).toOption)
          _       <- fake.image("iae/photo-app", "c7", "CVE-2026-1234", "CRITICAL", "openssl", "3.0.15")
          _       <- fake.image("iae/photo-app", "c1", "CVE-2025-0001", "HIGH", "zlib", "1.3.1")
          discovery: Discovery = (_, _) => ZIO.succeed(List(Discovered(web, None, None)))
          engine   = Engine.make(Drivers(List(Running)), discovery, AnyOrder).copy(findings = found)
          slack   <- InMemorySlack.make(Map("platform-owners" -> Set("U_BOB")))
          job      = SecurityJob(List(EnvRuntime(env, engine, ws)), List(app), slack, slack.self, channel, None,
                       owners = SlackGroup.from("platform-owners").toOption, images = Some(ImageScans(InspectorClient(AwsRestJsonClient(http, config)), Set(app))))
          _       <- job.findings *> job.findings
          posted  <- slack.posted(channel)
          threats  = posted.filter(_.text.startsWith("*Security threat*"))
          reports  = posted.filter(_.text.startsWith("*Security* `photo-app/prod`"))
        yield assertTrue(
          threats.size == 1,
          threats.head.text.contains("Role `runtime`: Credentials used from outside AWS (GuardDuty"),
          threats.head.text.contains("<!subteam^S_platform-owners>"),
          threats.head.text.contains("iae doesn't change anything for it"),
          reports.size == 1,
          reports.head.text.contains("2 finding(s)"),
          reports.head.text.contains("- [Critical] CVE-2026-1234 in openssl@1.0 (fixed in 3.0.15) in image `c7` (Inspector)"),
          reports.head.text.contains("- [High] Bucket `uploads`: public access isn't blocked (iae)"),
          !reports.head.text.contains("Something minor"),
          !reports.head.text.contains("CVE-2025-0001"),
        ),
  ).provideShared(Client.default.mapError(_.getMessage)) @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
