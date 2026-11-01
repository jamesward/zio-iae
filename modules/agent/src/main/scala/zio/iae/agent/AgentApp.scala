package zio.iae.agent

import java.nio.file.Paths
import zio.*
import zio.http.{Client, URL}
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.*
import zio.iae.engine.*
import zio.iae.engine.policy.RiskTableEvaluator

/** The always-on agent. Configured by `IAE_REPO_URL`, `IAE_BRANCH` (default `main`), `IAE_WORKDIR`, `IAE_INBOX_URL`,
  * `IAE_SLACK_TOKEN`, `IAE_SLACK_API` (default Slack's), `IAE_NAME_SUFFIX`, `IAE_RECONCILE_MINUTES` (default 15),
  * `IAE_APPROVAL_HOURS` (default 24) and the AWS variables. `POLICY.md` is evaluated by the model from
  * `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID` (and `IAE_BEDROCK_ENDPOINT` for a Converse-compatible origin); without
  * them the default risk table applies. The AWS credentials are the agent's own (the tooling account, where the inbox
  * is); envs with an account in `.iae/config.json` are reached through its roles. A change to the envs or accounts
  * takes a restart.
  */
object AgentApp extends ZIOAppDefault:
  private def env(name: String): IO[AgentError, Option[String]] =
    System.env(name).orElseFail(AgentError.Config(s"cannot read $name")).map(_.filter(_.nonEmpty))

  private def required(name: String): IO[AgentError, String] = env(name).someOrFail(AgentError.Config(s"$name is not set"))

  private def number(name: String, default: Int): IO[AgentError, Int] =
    env(name).flatMap(v => ZIO.fromOption(v.map(_.toIntOption)).orElseSucceed(Some(default)).someOrFail(AgentError.Config(s"$name must be a number")))

  /** CodeBuild with `IAE_BUILD_BUCKET` and `IAE_BUILD_ROLE` (the builds' service role), and optionally
    * `IAE_BUILD_IMAGE`; without them, builds run in the agent, which is only for development.
    */
  private def buildsFrom(base: AwsConfig, http: Client): IO[AgentError, ManifestSource] =
    env("IAE_BUILD_BUCKET").flatMap:
      case None =>
        ZIO.logWarning("IAE_BUILD_BUCKET is not set: builds run inside the agent, with its credentials (development only)").as(CommandManifests())
      case Some(name) =>
        for
          bucket <- ZIO.fromEither(BucketName.from(name)).mapError(AgentError.Config(_))
          role   <- required("IAE_BUILD_ROLE")
          image  <- env("IAE_BUILD_IMAGE").map(_.getOrElse("aws/codebuild/amazonlinux-x86_64-standard:5.0"))
          builds <- CodeBuildManifests.make(CodeBuildClient(AwsJsonClient(http, base)), S3Client(http, base), bucket, role, image)
        yield builds

  def run: ZIO[Any, Any, Any] =
    val program =
      for
        repo      <- required("IAE_REPO_URL")
        branch    <- env("IAE_BRANCH").map(_.getOrElse("main"))
        workdir   <- required("IAE_WORKDIR").map(Paths.get(_).nn)
        inboxUrl  <- required("IAE_INBOX_URL")
        token     <- required("IAE_SLACK_TOKEN")
        slackApi  <- env("IAE_SLACK_API").map(_.getOrElse(SlackWebApi.production.encode))
        slackBase <- ZIO.fromEither(URL.decode(slackApi)).orElseFail(AgentError.Config(s"IAE_SLACK_API is not a URL: $slackApi"))
        suffix    <- env("IAE_NAME_SUFFIX")
        every     <- number("IAE_RECONCILE_MINUTES", 15)
        expiry    <- number("IAE_APPROVAL_HOURS", 24)
        bootstrap  = GitWorkspace(workdir.resolve("bootstrap").nn, repo, branch)
        _         <- bootstrap.sync
        (config, _) <- RepoConfig.load(bootstrap.dir)
        base        <- ZIO.service[AwsConfig]
        envs <- ZIO.foreach(config.envs): name =>
                  val workspace = GitWorkspace(workdir.resolve(name.value).nn, repo, branch)
                  EnvRuntimes.make(name, config.accounts.get(name.value), base, BucketNaming(suffix), workspace)
        http        <- ZIO.service[Client]
        slack        = SlackWebApi(http, SlackToken(token), slackBase)
        self        <- slack.identity
        model       <- Model.fromEnvironment
        evaluator   <- ZIO.foreach(model)(EvaluatePolicy.make).someOrElseZIO:
                         ZIO.logWarning("no Bedrock model configured: using the default risk table instead of POLICY.md")
                           .as(RiskTableEvaluator(config.slack.groups.headOption.getOrElse(config.slack.owners)))
        explainer   <- ZIO.foreach(model)(ExplainPlan.make)
        manifests   <- buildsFrom(base, http)
        pipeline     = Pipeline(manifests, evaluator, slack, self, expiry.hours, explainer)
        reconcilers <- Reconcilers.make(pipeline, envs)
        sqs         <- ZIO.service[SqsClient]
        _           <- ZIO.logInfo(s"agent started for $repo ($branch): envs ${config.envs.map(_.value).mkString(", ")}")
        _           <- Agent(reconcilers, InboxConsumer(sqs, QueueUrl(inboxUrl), branch, reconcilers), every.minutes).run
      yield ()
    ZIO
      .scoped(program)
      .provide(AwsConfig.layer, Client.default, AwsJsonClient.layer, SqsClient.layer)
      .tapError:
        case e: AgentError => ZIO.logError(e.message)
        case other         => ZIO.logError(other.toString)
