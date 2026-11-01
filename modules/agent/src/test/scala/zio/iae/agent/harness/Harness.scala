package zio.iae.agent.harness

import zio.*
import zio.http.*
import zio.iae.*
import zio.iae.agent.*
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.*
import zio.iae.engine.*
import zio.iae.engine.policy.{RiskTableEvaluator, SlackGroup}
import zio.iae.testkit.{AuditManagerFake, FlociLayer}
import zio.iae.webhook.*

/** The local harness: the real agent and webhook, with GitHub and Slack emulated and AWS on Floci. Only the LLM is
  * real: with `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID` set, `POLICY.md` is evaluated by that model, otherwise
  * by the default risk table.
  */
final case class Harness(
    port: Int,
    ui: HarnessUi,
    slack: FakeSlack,
    repo: TestRepo,
    envs: List[EnvRuntime],
    clock: HarnessClock,
    agent: AgentProcess,
    s3: List[S3Client],
    audit: AuditManagerFake,
)

/** The agent as a process the harness can stop and start; a restart loses everything in memory, like a new
  * container would.
  */
final class AgentProcess(start: IO[String, (Fiber.Runtime[Nothing, Nothing], Reconcilers)], running: Ref.Synchronized[Option[(Fiber.Runtime[Nothing, Nothing], Reconcilers)]]):
  def signal: UIO[Unit]          = running.get.flatMap(r => ZIO.foreachDiscard(r)(_._2.signalAll))
  def restart: IO[String, Unit] = running.updateZIO(r => ZIO.foreachDiscard(r)(_._1.interrupt) *> start.map(Some(_)))

object AgentProcess:
  def make(start: IO[String, (Fiber.Runtime[Nothing, Nothing], Reconcilers)]): IO[String, AgentProcess] =
    for
      running <- Ref.Synchronized.make(Option.empty[(Fiber.Runtime[Nothing, Nothing], Reconcilers)])
      process  = AgentProcess(start, running)
      _       <- process.restart
    yield process

object Harness:
  object Uploads extends Bucket.Id(LogicalId("uploads"))

  val app      = AppName("photo-app")
  val channel  = "C_PHOTOS"
  val manifest = ".iae/manifest.json"
  val users    = List(FakeUser("U_ALICE", "alice"), FakeUser("U_BOB", "bob"), FakeUser("U_CAROL", "carol"), FakeUser("U_DEV", "dev"))
  val groups   = Map("infra-approvers" -> Set("U_ALICE"), "platform-owners" -> Set("U_BOB"))
  val policy =
    """# Approval policy
      |
      |Approvers are the @infra-approvers Slack group. Owners are @platform-owners.
      |
      |- Low and medium risk changes are fine outside prod; high risk needs an approver.
      |- In prod, anything above low risk needs an approver; critical needs two owners.
      |""".stripMargin

  def config(envs: List[EnvName]): String =
    s"""{"apps":[{"name":"${app.value}","manifest":["cat","$manifest"],"image":"alpine:3.20"}],
       |"envs":[${envs.map(e => s"\"${e.value}\"").mkString(",")}],"production":["${envs.last.value}"],
       |"slack":{"channel":"$channel","owners":"platform-owners","groups":["infra-approvers"]},
       |"identities":{"dev@example.com":"U_DEV"}}""".stripMargin

  /** Creates an env account's planner and deployer roles, trusting the account's root, and its evidence bucket, as
    * `iae bootstrap` will. The assessment's ids are the ones the harness's Audit Manager fake accepts.
    */
  def bootstrapAccount(env: EnvName, config: AwsConfig): ZIO[Client, String, EnvAccount] =
    for
      http    <- ZIO.service[Client]
      iam      = IamClient(AwsQueryClient(http, config))
      account <- StsClient(AwsQueryClient(http, config)).accountId.mapError(_.message)
      trust    = s"""{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::${account.value}:root"},"Action":"sts:AssumeRole"}]}"""
      arns <- ZIO.foreach(List("iae-planner", "iae-deployer")): role =>
                val name = RoleName.from(role).toOption.get
                iam.getRole(name).flatMap(r => iam.createRole(name, "/", trust, Map.empty).when(r.isEmpty)).mapError(_.message)
                  .as(s"arn:aws:iam::${account.value}:role/$role")
      bucket   = BucketName.from(s"iae-evidence-${env.value}").toOption.get
      s3       = S3Client(http, config)
      exists  <- s3.bucketExists(bucket).mapError(_.toString)
      _       <- s3.createBucket(bucket).mapError(_.toString).unless(exists)
      audit    = AuditSettings(bucket.value, s"assessment-${env.value}", "IAE Change Management", Map("IAE-CM-1" -> "iae-cm-1", "IAE-CM-3" -> "iae-cm-3"))
    yield EnvAccount(arns(0), arns(1), audit = Some(audit))

  /** `speed` is the agent's clock speed factor: at 60, a day passes in 24 minutes. Envs in `accounts` get their own
    * account (a Floci of their own), reached through assumed roles like production; the others share the tooling
    * account's.
    */
  def make(port: Int, envNames: List[EnvName], speed: Double = 60, accounts: Map[EnvName, AwsConfig] = Map.empty)
      : ZIO[Scope & AwsConfig & SqsClient & Client, String, Harness] =
    val githubSecret = WebhookSecret("harness-github")
    val slackSecret  = WebhookSecret("harness-slack")
    for
      repo     <- TestRepo.make
      _        <- repo.commit(Map(".iae/config.json" -> config(envNames), RepoConfig.policyPath -> policy, manifest -> Bucket.readWrite(Uploads).manifest(app).toOption.get.toJson), "seed")
      sqs      <- ZIO.service[SqsClient]
      inboxUrl <- sqs.createQueue(s"iae-harness-inbox-${envNames.map(_.value).mkString("-")}", Map.empty, Map.empty).mapError(_.message)
      webhook   = Webhook(WebhookSettings(githubSecret, slackSecret), SqsInbox(sqs, inboxUrl))
      clock    <- HarnessClock.make(speed)
      started  <- Promise.make[Nothing, Harness]
      slack    <- FakeSlack.make(channel, users, groups, webhook, slackSecret, clock)
      ui       <- HarnessUi.make(slack, repo, webhook, githubSecret, List(manifest, RepoConfig.policyPath, ".iae/config.json"), clock, started.await)
      server   <- Server.defaultWithPort(port).build.mapError(_.getMessage)
      bound    <- Server.install(slack.api ++ ui.routes).provideEnvironment(server)
      http     <- ZIO.service[Client]
      base     <- ZIO.fromEither(URL.decode(s"http://localhost:$bound/api")).mapError(_.getMessage)
      client    = SlackWebApi(http, SlackToken("xoxb-harness"), base)
      self     <- client.identity.mapError(_.message)
      tooling  <- ZIO.service[AwsConfig]
      audit    <- AuditManagerFake.make.mapError(_.getMessage)
      envs <- ZIO.foreach(envNames): env =>
                val base = accounts.getOrElse(env, tooling)
                for
                  account <- ZIO.foreach(accounts.get(env))(bootstrapAccount(env, _))
                  runtime <- EnvRuntimes.make(env, account, base, BucketNaming(None), repo.workspace(env), Endpoint.parse(audit.url.encode).toOption).mapError(_.message)
                yield runtime
      // Builds run in the tooling account's CodeBuild (Floci runs them as containers).
      buildsAt  = BucketName.from(s"iae-builds-${envNames.head.value}").toOption.get
      builds   <- CodeBuildManifests.make(CodeBuildClient(AwsJsonClient(http, tooling)), S3Client(http, tooling), buildsAt, "arn:aws:iam::000000000000:role/iae-build", "alpine:3.20")
                    .mapError(_.message)
      scope    <- ZIO.scope
      httpEnv  <- ZIO.environment[Client]
      start = (for
                model     <- Model.fromEnvironment.provideEnvironment(httpEnv).mapError(_.message)
                evaluator <- ZIO.foreach(model)(EvaluatePolicy.make).mapError(_.message).someOrElseZIO:
                               ZIO.logInfo("no Bedrock model configured: the harness uses the default risk table instead of POLICY.md")
                                 .as(RiskTableEvaluator(SlackGroup.from("infra-approvers").toOption.get))
                explainer <- ZIO.foreach(model)(ExplainPlan.make).mapError(_.message)
                pipeline     = Pipeline(builds, evaluator, client, self, 24.hours, explainer)
                reconcilers <- Reconcilers.make(pipeline, envs)
                fiber       <- ZIO.withClock(clock)(Agent(reconcilers, InboxConsumer(sqs, inboxUrl, "main", reconcilers), 15.minutes).run).forkIn(scope)
              yield (fiber, reconcilers))
      agent    <- AgentProcess.make(start)
      s3        = envNames.map(env => S3Client(http, accounts.getOrElse(env, tooling)))
      harness   = Harness(bound, ui, slack, repo, envs, clock, agent, s3, audit)
      _        <- started.succeed(harness)
    yield harness

/** `./sbta agent/Test/run`: the harness on http://localhost:8088 (or `IAE_HARNESS_PORT`). Needs Docker. */
object AppTest extends ZIOAppDefault:
  def run: ZIO[Any, Any, Any] =
    val program =
      for
        port    <- System.env("IAE_HARNESS_PORT").map(_.flatMap(_.toIntOption).getOrElse(8088))
        staging <- FlociLayer.live.build.map(_.get[AwsConfig])
        prod    <- FlociLayer.live.build.map(_.get[AwsConfig])
        envs     = List(EnvName("staging"), EnvName("prod"))
        harness <- Harness.make(port, envs, accounts = envs.zip(List(staging, prod)).toMap)
        _       <- ZIO.logInfo(s"iae harness running: open http://localhost:${harness.port} (staging and prod each have their own Floci)")
        _       <- ZIO.never
      yield ()
    ZIO.scoped(program).provide(FlociLayer.live, Client.default, AwsJsonClient.layer, SqsClient.layer)
