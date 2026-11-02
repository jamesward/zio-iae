package zio.iae.aws

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.{FlociLayer, SecurityFake}
import zio.test.*

/** Security findings about an app's resources: the engine's own checks on Floci, and Security Hub, GuardDuty and
  * Access Analyzer (a fake), matched to the resources by name.
  */
object FindingsSpec extends ZIOSpecDefault:
  val app = AppName("photo-app")

  def spec = suite("Findings")(
    test("findings are matched to the app's resources, threats marked, duplicates of Security Hub's dropped"):
      ZIO.scoped:
        for
          env     <- FlociSpec.uniqueEnv
          s3      <- ZIO.service[S3Client]
          aws     <- ZIO.service[AwsConfig]
          http    <- ZIO.service[Client]
          uploads  = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
          jobs     = Address(app, env, ResourceKind.Queue, LogicalId("jobs"))
          role     = Address(app, env, ResourceKind.Role, Manifest.runtimeRole)
          sqs      = SqsClient(AwsJsonClient(http, aws))
          _       <- BucketProvider(s3, BucketNaming(None)).applyChange(Change.Create(uploads, BucketSpec()), ApplyContext.local)
          _       <- QueueProvider(sqs).applyChange(Change.Create(jobs, QueueSpec()), ApplyContext.local)
          bucket   = BucketNaming(None).physical(uploads).toOption.get.value
          roleName = RoleProvider.roleName(app, env).toOption.get.value
          fake    <- SecurityFake.make.orDie
          _       <- fake.hub("hub-1", "Security Hub", "HIGH", "S3 general purpose buckets should block public access", s"arn:aws:s3:::$bucket")
          _       <- fake.hub("arn:aws:guardduty:us-east-1:0:detector/d1/finding/gd-1", "GuardDuty", "HIGH", "Credentials used from outside AWS", s"arn:aws:iam::0:role/iae/$roleName")
          _       <- fake.hub("hub-low", "Security Hub", "INFORMATIONAL", "Something minor", s"arn:aws:s3:::$bucket")
          _       <- fake.guardDuty("gd-1", 8, "Credentials used from outside AWS", roleName)
          _       <- fake.guardDuty("gd-2", 5, "Unusual API calls", roleName)
          _       <- fake.guardDuty("gd-other", 8, "Someone else's problem", "another-teams-role")
          _       <- fake.analyzer("aa-1", s"arn:aws:s3:::$bucket", public = true)
          faked    = AwsRestJsonClient(http, aws.copy(endpoint = Endpoint.parse(fake.url.encode).toOption))
          found: Discovery = (_, _) => ZIO.succeed(List(Discovered(uploads, None, None), Discovered(jobs, None, None), Discovered(role, None, None)))
          checks   = ConfigChecks(s3, sqs, Elbv2Client(AwsQueryClient(http, aws)), BucketNaming(None))
          findings = AwsFindings(found, BucketNaming(None), checks, SecurityHubClient(faked), GuardDutyClient(faked), AccessAnalyzerClient(faked))
          result  <- findings.security(app, env)
          own     <- checks.check(uploads).zip(checks.check(jobs))
        yield
          val bySource = result.filter(_.source != "iae").map(f => (f.source, f.id, f.severity, f.resource.map(_.kind), f.threat)).toSet
          assertTrue(
            bySource == Set(
              ("Security Hub", "hub-1", Severity.High, Some(ResourceKind.Bucket), false),
              ("GuardDuty", "arn:aws:guardduty:us-east-1:0:detector/d1/finding/gd-1", Severity.High, Some(ResourceKind.Role), true),
              ("GuardDuty", "gd-2", Severity.Medium, Some(ResourceKind.Role), false),
              ("Access Analyzer", "aa-1", Severity.High, Some(ResourceKind.Bucket), false),
            ),
            result.filter(_.source == "iae").toSet == (own._1 ++ own._2).toSet,
          )
    ,
    test("iae's own checks: public access, encryption at rest"):
      for
        env  <- FlociSpec.uniqueEnv
        s3   <- ZIO.service[S3Client]
        aws  <- ZIO.service[AwsConfig]
        http <- ZIO.service[Client]
        sqs   = SqsClient(AwsJsonClient(http, aws))
        jobs  = Address(app, env, ResourceKind.Queue, LogicalId("jobs"))
        _    <- QueueProvider(sqs).applyChange(Change.Create(jobs, QueueSpec()), ApplyContext.local)
        url  <- sqs.queueUrl(QueueNaming.physical(jobs).toOption.get).someOrFail("no queue")
        before <- ConfigChecks(s3, sqs, Elbv2Client(AwsQueryClient(http, aws)), BucketNaming(None)).check(jobs)
        _    <- sqs.setAttributes(url, Map("SqsManagedSseEnabled" -> "true"))
        after <- ConfigChecks(s3, sqs, Elbv2Client(AwsQueryClient(http, aws)), BucketNaming(None)).check(jobs)
      yield assertTrue(after.isEmpty, before.forall(_.title == "messages aren't encrypted at rest")),
  ).provideShared(FlociLayer.live, Client.default, AwsCloud.clients) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ TestAspect.timeout(3.minutes)
