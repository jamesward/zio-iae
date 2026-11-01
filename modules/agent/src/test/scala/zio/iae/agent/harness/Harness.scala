package zio.iae.agent.harness

import zio.*
import zio.http.*
import zio.iae.*
import zio.iae.agent.*
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.SqsClient
import zio.iae.engine.*
import zio.iae.engine.policy.{RiskTableEvaluator, SlackGroup}
import zio.iae.testkit.FlociLayer
import zio.iae.webhook.*

/** The local harness: the real agent and webhook, with GitHub and Slack emulated and AWS on Floci. Only the LLM is
  * real: with `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID` set, `POLICY.md` is evaluated by that model, otherwise
  * by the default risk table.
  */
final case class Harness(port: Int, ui: HarnessUi, slack: FakeSlack, repo: TestRepo, envs: List[EnvRuntime])

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
    s"""{"apps":[{"name":"${app.value}","manifest":["cat","$manifest"]}],
       |"envs":[${envs.map(e => s"\"${e.value}\"").mkString(",")}],"production":["${envs.last.value}"],
       |"slack":{"channel":"$channel","owners":"platform-owners","groups":["infra-approvers"]},
       |"identities":{"dev@example.com":"U_DEV"}}""".stripMargin

  def make(port: Int, envNames: List[EnvName]): ZIO[Scope & AwsEngine.Clients & Client, String, Harness] =
    val githubSecret = WebhookSecret("harness-github")
    val slackSecret  = WebhookSecret("harness-slack")
    for
      repo     <- TestRepo.make
      _        <- repo.commit(Map(".iae/config.json" -> config(envNames), RepoConfig.policyPath -> policy, manifest -> Bucket.readWrite(Uploads).manifest(app).toOption.get.toJson), "seed")
      sqs      <- ZIO.service[SqsClient]
      inboxUrl <- sqs.createQueue(s"iae-harness-inbox-$port", Map.empty, Map.empty).mapError(_.message)
      webhook   = Webhook(WebhookSettings(githubSecret, slackSecret), SqsInbox(sqs, inboxUrl))
      slack    <- FakeSlack.make(channel, users, groups, webhook, slackSecret)
      ui        = HarnessUi(slack, repo, webhook, githubSecret, List(manifest, RepoConfig.policyPath, ".iae/config.json"))
      server   <- Server.defaultWithPort(port).build.mapError(_.getMessage)
      bound    <- Server.install(slack.api ++ ui.routes).provideEnvironment(server)
      http     <- ZIO.service[Client]
      base     <- ZIO.fromEither(URL.decode(s"http://localhost:$bound/api")).mapError(_.getMessage)
      client    = SlackWebApi(http, SlackToken("xoxb-harness"), base)
      self     <- client.identity.mapError(_.message)
      envs <- ZIO.foreach(envNames): env =>
                val workspace = repo.workspace(env)
                AwsEngine.layer(BucketNaming(None)).build
                  .provideSome[AwsEngine.Clients & Scope](ZLayer.succeed[CommitOrder](GitCommitOrder(workspace.dir)))
                  .map(e => EnvRuntime(env, e.get, workspace))
      evaluator   <- EvaluatePolicy.fromEnvironment.mapError(_.message).someOrElseZIO:
                       ZIO.logInfo("no Bedrock model configured: the harness uses the default risk table instead of POLICY.md")
                         .as(RiskTableEvaluator(SlackGroup.from("infra-approvers").toOption.get))
      pipeline     = Pipeline(CommandManifests(), evaluator, client, self, 24.hours)
      reconcilers <- Reconcilers.make(pipeline, envs)
      _           <- Agent(reconcilers, InboxConsumer(sqs, inboxUrl, "main", reconcilers), 15.minutes).run.forkScoped
    yield Harness(bound, ui, slack, repo, envs)

/** `./sbta agent/Test/run`: the harness on http://localhost:8088 (or `IAE_HARNESS_PORT`). Needs Docker. */
object AppTest extends ZIOAppDefault:
  def run: ZIO[Any, Any, Any] =
    val program =
      for
        port    <- System.env("IAE_HARNESS_PORT").map(_.flatMap(_.toIntOption).getOrElse(8088))
        harness <- Harness.make(port, List(EnvName("staging"), EnvName("prod")))
        _       <- ZIO.logInfo(s"iae harness running: open http://localhost:${harness.port}")
        _       <- ZIO.never
      yield ()
    ZIO.scoped(program).provide(FlociLayer.live, Client.default, AwsEngine.clients)
