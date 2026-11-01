package zio.iae.executor

import java.nio.file.Paths
import zio.*
import zio.http.{Client, URL}
import zio.iae.agent.*
import zio.iae.agent.AgentApp.{env, number, required}
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.*
import zio.iae.webhook.SqsInbox

/** The executor service: the only principal that assumes the env accounts' deployer roles. Configured by
  * `IAE_REPO_URL`, `IAE_BRANCH` (default `main`), `IAE_WORKDIR`, `IAE_EXECUTOR_QUEUE_URL` (its requests),
  * `IAE_INBOX_URL` (the agent's inbox, to wake it after an apply), `IAE_SLACK_TOKEN` (its own Slack app's token),
  * `IAE_SLACK_API`, `IAE_AGENT_BOT_ID` and `IAE_AGENT_USER_ID` (the agent's Slack identity, the only author of
  * approval requests), `IAE_NAME_SUFFIX`, `IAE_APPROVAL_HOURS` (default 24), the build bucket settings of `AgentApp`
  * (to read built manifests) and the AWS variables. It has no model.
  */
object ExecutorApp extends ZIOAppDefault:
  def run: ZIO[Any, Any, Any] =
    val program =
      for
        repo        <- required("IAE_REPO_URL")
        branch      <- env("IAE_BRANCH").map(_.getOrElse("main"))
        workdir     <- required("IAE_WORKDIR").map(Paths.get(_).nn)
        queueUrl    <- required("IAE_EXECUTOR_QUEUE_URL")
        inboxUrl    <- required("IAE_INBOX_URL")
        token       <- required("IAE_SLACK_TOKEN")
        slackApi    <- env("IAE_SLACK_API").map(_.getOrElse(SlackWebApi.production.encode))
        slackBase   <- ZIO.fromEither(URL.decode(slackApi)).orElseFail(AgentError.Config(s"IAE_SLACK_API is not a URL: $slackApi"))
        agentBot    <- required("IAE_AGENT_BOT_ID")
        agentUser   <- required("IAE_AGENT_USER_ID")
        suffix      <- env("IAE_NAME_SUFFIX")
        expiry      <- number("IAE_APPROVAL_HOURS", 24)
        bootstrap    = GitWorkspace(workdir.resolve("bootstrap").nn, repo, branch)
        _           <- bootstrap.sync
        (config, _) <- RepoConfig.load(bootstrap.dir)
        base        <- ZIO.service[AwsConfig]
        envs <- ZIO.foreach(config.envs): name =>
                  EnvRuntimes.make(name, config.accounts.get(name.value), base, BucketNaming(suffix), GitWorkspace(workdir.resolve(name.value).nn, repo, branch))
        http        <- ZIO.service[Client]
        sqs         <- ZIO.service[SqsClient]
        manifests   <- AgentApp.buildsFrom(base, http)
        executor     = ExecutorService(SlackWebApi(http, SlackToken(token), slackBase), SlackIdentity(agentBot, agentUser), envs, manifests, SqsInbox(sqs, QueueUrl(inboxUrl)), expiry.hours)
        _           <- ZIO.logInfo(s"executor started for $repo ($branch): envs ${config.envs.map(_.value).mkString(", ")}")
        _           <- executor.consume(sqs, QueueUrl(queueUrl))
      yield ()
    ZIO
      .scoped(program)
      .provide(AwsConfig.layer, Client.default, AwsJsonClient.layer, SqsClient.layer)
      .tapError:
        case e: AgentError => ZIO.logError(e.message)
        case other         => ZIO.logError(other.toString)
