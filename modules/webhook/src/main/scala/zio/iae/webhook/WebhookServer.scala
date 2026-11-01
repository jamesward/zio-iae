package zio.iae.webhook

import zio.*
import zio.http.*
import zio.iae.aws.client.*

/** Runs the webhook as an HTTP server: on Lambda behind the Lambda Web Adapter, or anywhere else. Configured by
  * `IAE_GITHUB_WEBHOOK_SECRET`, `IAE_SLACK_SIGNING_SECRET`, `IAE_INBOX_URL`, `PORT` and the AWS variables.
  */
object WebhookServer extends ZIOAppDefault:
  private def required(name: String): IO[String, String] =
    System.env(name).orElseFail(s"cannot read $name").someOrFail(s"$name is not set")

  def run: ZIO[Any, Any, Any] =
    val program =
      for
        github <- required("IAE_GITHUB_WEBHOOK_SECRET")
        slack  <- required("IAE_SLACK_SIGNING_SECRET")
        inbox  <- required("IAE_INBOX_URL")
        port   <- System.env("PORT").orElseFail("cannot read PORT").map(_.flatMap(_.toIntOption).getOrElse(8080))
        sqs    <- ZIO.service[SqsClient]
        hook    = Webhook(WebhookSettings(WebhookSecret(github), WebhookSecret(slack)), SqsInbox(sqs, QueueUrl(inbox)))
        _      <- Server.serve(hook.routes).provide(Server.defaultWithPort(port))
      yield ()
    program.provide(AwsConfig.layer, Client.default, AwsJsonClient.layer, SqsClient.layer)
