package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, Message, RequestConfig}
import zio.*
import zio.iae.engine.{Plan, PlanText}
import zio.iae.engine.policy.PlanDecision

/** Explains a plan to the people asked to approve it. */
trait PlanExplainer:
  def explain(plan: Plan, decision: PlanDecision): IO[String, String]

/** The `ExplainPlan` task: a plain-language explanation of an approval request, posted in its thread. It informs
  * the approvers and decides nothing.
  */
final case class ExplainPlan(bedrock: Bedrock, instructions: String) extends PlanExplainer:
  def explain(plan: Plan, decision: PlanDecision): IO[String, String] =
    Bedrock
      .chat(RequestConfig(List(Message.user(ExplainPlan.prompt(plan, decision))), instructions, InferenceConfig(maxTokens = 1024, temperature = 0.2)))
      .text
      .provideEnvironment(ZEnvironment(bedrock))
      .mapError(_.getMessage)
      .map(_.trim)
      .filterOrFail(_.nonEmpty)("the model returned no explanation")

object ExplainPlan:
  def prompt(plan: Plan, decision: PlanDecision): String =
    s"""<plan>
       |${PlanText.render(plan)}
       |</plan>
       |
       |<policy>
       |${PlanText.render(decision)}
       |</policy>""".stripMargin

  def make(bedrock: Bedrock): IO[AgentError, PlanExplainer] =
    Model.prompt("explain-plan").mapBoth(AgentError.Config(_), ExplainPlan(bedrock, _))
