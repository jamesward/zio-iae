package zio.iae.agent

import com.jamesward.zio_bedrock.{Bedrock, Converse, ConverseConfig}
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId, Region}
import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.{Client, URL}

/** The model the agent's LLM tasks use, through zio-bedrock's Converse backend. */
object Model:
  /** From `AWS_BEARER_TOKEN_BEDROCK`, `BEDROCK_MODEL_ID` and `AWS_REGION`, or `IAE_BEDROCK_ENDPOINT` for a
    * Converse-compatible origin (acprock, or a fake). None when no model is configured.
    */
  val fromEnvironment: ZIO[Client, AgentError, Option[Bedrock]] =
    def env(name: String) = System.env(name).orElseFail(AgentError.Config(s"cannot read $name")).map(_.filter(_.nonEmpty))
    for
      key      <- env("AWS_BEARER_TOKEN_BEDROCK")
      model    <- env("BEDROCK_MODEL_ID")
      region   <- env("AWS_REGION").map(_.getOrElse(Region.UsEast1.code))
      endpoint <- env("IAE_BEDROCK_ENDPOINT")
      config <- (key, model) match
                  case (Some(k), Some(m)) =>
                    endpoint match
                      case Some(e) =>
                        ZIO.fromEither(URL.decode(e)).orElseFail(AgentError.Config(s"IAE_BEDROCK_ENDPOINT is not a URL: $e"))
                          .map(url => Some(ConverseConfig(ApiKey(k), url, ModelId(m))))
                      case None =>
                        ZIO.fromOption(Region.fromCode(region)).orElseFail(AgentError.Config(s"AWS_REGION $region has no Bedrock"))
                          .map(r => Some(ConverseConfig(ApiKey(k), r, ModelId(m))))
                  case _ => ZIO.none
      bedrock <- ZIO.foreach(config)(make)
    yield bedrock

  def make(config: ConverseConfig): URIO[Client, Bedrock] = ZIO.scoped(Converse.layer(config).build).map(_.get[Bedrock])

  /** A task's system prompt, from `prompts/<task>.md` in the agent's resources. */
  def prompt(task: String): IO[String, String] =
    ZIO
      .attemptBlocking(Option(getClass.getResourceAsStream(s"/prompts/$task.md")).map(s => String(s.readAllBytes(), UTF_8)))
      .orElseFail(s"cannot read prompts/$task.md")
      .someOrFail(s"prompts/$task.md is missing")
