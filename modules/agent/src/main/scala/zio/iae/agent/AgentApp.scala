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
  * takes a restart. With `IAE_EXECUTOR_QUEUE_URL` the agent never applies: it plans with the env accounts' planner
  * roles only and sends what it would apply to the executor (`ExecutorApp`).
  */
object AgentApp extends ZIOAppDefault:
  def env(name: String): IO[AgentError, Option[String]] =
    System.env(name).orElseFail(AgentError.Config(s"cannot read $name")).map(_.filter(_.nonEmpty))

  def required(name: String): IO[AgentError, String] = env(name).someOrFail(AgentError.Config(s"$name is not set"))

  def number(name: String, default: Int): IO[AgentError, Int] =
    env(name).flatMap(v => ZIO.fromOption(v.map(_.toIntOption)).orElseSucceed(Some(default)).someOrFail(AgentError.Config(s"$name must be a number")))

  /** CodeBuild with `IAE_BUILD_BUCKET` and `IAE_BUILD_ROLE` (the builds' service role), and optionally
    * `IAE_BUILD_IMAGE`; without them, builds run in the agent, which is only for development. Apps whose build pushes
    * their image get their repositories in ECR in the agent's account.
    */
  def buildsFrom(base: AwsConfig, http: Client): IO[AgentError, ManifestSource] =
    env("IAE_BUILD_BUCKET").flatMap:
      case None =>
        ZIO.logWarning("IAE_BUILD_BUCKET is not set: builds run inside the agent, with its credentials (development only)").as(CommandManifests())
      case Some(name) =>
        for
          bucket <- ZIO.fromEither(BucketName.from(name)).mapError(AgentError.Config(_))
          role   <- required("IAE_BUILD_ROLE")
          image  <- env("IAE_BUILD_IMAGE").map(_.getOrElse("aws/codebuild/amazonlinux-x86_64-standard:5.0"))
          builds <- CodeBuildManifests.make(CodeBuildClient(AwsJsonClient(http, base)), S3Client(http, base), bucket, role, image, Some(EcrClient(AwsJsonClient(http, base))))
        yield builds

  /** The GitHub App the agent opens pull requests as: `IAE_GITHUB_APP_ID`, `IAE_GITHUB_INSTALLATION_ID` and the private
    * key in `IAE_GITHUB_APP_KEY` (PEM) or `IAE_GITHUB_APP_KEY_FILE`; `IAE_GITHUB_API` for GitHub Enterprise. Without
    * them the agent diagnoses failures but doesn't propose fixes.
    */
  private def pullRequests(repoUrl: String, http: Client): IO[AgentError, Option[PullRequests]] =
    for
      appId        <- env("IAE_GITHUB_APP_ID")
      installation <- env("IAE_GITHUB_INSTALLATION_ID")
      keyText      <- env("IAE_GITHUB_APP_KEY")
      keyFile      <- env("IAE_GITHUB_APP_KEY_FILE")
      key          <- ZIO.foreach(keyText.map(Right(_)).orElse(keyFile.map(Left(_)))):
                        case Right(k)   => ZIO.succeed(k)
                        case Left(file) => ZIO.attemptBlocking(java.nio.file.Files.readString(Paths.get(file)).nn).mapError(e => AgentError.Config(s"reading $file: ${e.getMessage}"))
      api          <- env("IAE_GITHUB_API").map(_.getOrElse("https://api.github.com")).flatMap(a => ZIO.fromEither(URL.decode(a)).orElseFail(AgentError.Config(s"IAE_GITHUB_API is not a URL: $a")))
      prs <- (appId, installation, key) match
               case (Some(id), Some(inst), Some(pem)) =>
                 for
                   repo <- ZIO.fromEither(GitHubRepo.fromUrl(repoUrl)).mapError(AgentError.Config(_))
                   app  <- GitHubApp.make(http, api, id, pem, inst).mapError(AgentError.Config(_))
                 yield Some(GitHubPullRequests(app, repo))
               case _ => ZIO.logWarning("no GitHub App configured: failures are diagnosed but no fixes are proposed").as(None)
    yield prs

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
        executorUrl <- env("IAE_EXECUTOR_QUEUE_URL")
        envs <- ZIO.foreach(config.envs): name =>
                  val workspace = GitWorkspace(workdir.resolve(name.value).nn, repo, branch)
                  EnvRuntimes.make(name, config.accounts.get(name.value), base, BucketNaming(suffix), workspace, canApply = executorUrl.isEmpty)
        sqs         <- ZIO.service[SqsClient]
        http        <- ZIO.service[Client]
        slack        = SlackWebApi(http, SlackToken(token), slackBase)
        self        <- slack.identity
        model       <- Model.fromEnvironment
        evaluator   <- ZIO.foreach(model)(EvaluatePolicy.make).someOrElseZIO:
                         ZIO.logWarning("no Bedrock model configured: using the default risk table instead of POLICY.md")
                           .as(RiskTableEvaluator(config.slack.groups.headOption.getOrElse(config.slack.owners)))
        explainer   <- ZIO.foreach(model)(ExplainPlan.make)
        prs         <- pullRequests(repo, http)
        diagnoser   <- ZIO.foreach(model)(DiagnoseFailure.make(_, prs))
        manifests   <- buildsFrom(base, http)
        pipeline     = Pipeline(manifests, evaluator, slack, self, expiry.hours, explainer, diagnoser, executorUrl.map(u => SqsApplyQueue(sqs, QueueUrl(u))))
        reconcilers <- Reconcilers.make(pipeline, envs)
        talk        <- ZIO.foreach(model)(m => Converse.make(slack, self, m, envs, manifests).flatMap(Conversations.make))
        triage      <- ZIO.foreach(model)(Triage.make(_, prs))
        monitor     <- Monitor.make(envs, config.apps.map(_.name), slack, self, config.slack.channel, config.slack.owners, triage)
        _           <- ZIO.logInfo(s"agent started for $repo ($branch): envs ${config.envs.map(_.value).mkString(", ")}")
        _           <- Agent(reconcilers, InboxConsumer(sqs, QueueUrl(inboxUrl), branch, reconcilers, talk), every.minutes, Some(monitor)).run
      yield ()
    ZIO
      .scoped(program)
      .provide(AwsConfig.layer, Client.default, AwsJsonClient.layer, SqsClient.layer)
      .tapError:
        case e: AgentError => ZIO.logError(e.message)
        case other         => ZIO.logError(other.toString)
