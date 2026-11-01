package zio.iae.agent

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, Message, RequestConfig}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.ZoneOffset
import zio.*
import zio.iae.engine.policy.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.simpleEnum
import zio.schema.codec.JsonCodec

/** The `EvaluatePolicy` task: a model reads `POLICY.md` and the change facts and returns one verdict per change, as
  * structured output. Everything it returns is checked by `Verdicts.decide`, so a bad answer ends in an owner
  * approval, never an apply.
  */
final case class EvaluatePolicy(bedrock: Bedrock, instructions: String) extends PolicyEvaluator:
  import EvaluatePolicy.*

  def evaluate(policy: PolicyConfig, facts: List[ChangeFacts]): IO[String, List[ChangeVerdict]] =
    if facts.isEmpty then ZIO.succeed(Nil)
    else
      Bedrock
        .chat(RequestConfig(List(Message.user(prompt(facts))), system(policy), InferenceConfig(maxTokens = 4096, temperature = 0.0)))
        .as[ModelVerdicts]
        .provideEnvironment(ZEnvironment(bedrock))
        .mapBoth(_.getMessage, _.verdicts.map(toVerdict(policy)))

  private def system(policy: PolicyConfig): String =
    s"""$instructions
       |
       |Slack groups the policy may name: ${policy.groups.toList.sorted.map(_.handle).mkString(", ")}.
       |
       |<policy>
       |${policy.text}
       |</policy>""".stripMargin

object EvaluatePolicy:
  /** Decision as the model writes it; the counts and groups are separate fields so the schema stays flat. */
  @simpleEnum
  enum ModelDecision derives Schema, CanEqual:
    case Auto, Approve, Deny

  final case class ModelVerdict(
      change: String,
      decision: ModelDecision,
      approvals: Int,
      groups: List[String],
      clauses: List[String],
      reasoning: String,
      confident: Boolean,
  ) derives Schema

  final case class ModelVerdicts(verdicts: List[ModelVerdict]) derives Schema

  private val factsCodec = JsonCodec.jsonEncoder(Schema[List[ChangeFacts]])

  def prompt(facts: List[ChangeFacts]): String =
    s"""Planned changes, as JSON. Return one verdict per change.
       |
       |<changes>
       |${factsCodec.encodeJson(facts, Some(2))}
       |</changes>""".stripMargin

  /** A group that isn't a valid handle makes the verdict doubtful, which `Verdicts.decide` turns into owners. */
  def toVerdict(policy: PolicyConfig)(v: ModelVerdict): ChangeVerdict =
    val groups = v.groups.map(SlackGroup.from)
    val bad    = groups.collect { case Left(e) => e }
    val decision = v.decision match
      case ModelDecision.Auto    => Decision.Auto
      case ModelDecision.Deny    => Decision.Deny
      case ModelDecision.Approve =>
        val named = groups.collect { case Right(g) => g }.toSet
        Decision.Approve(v.approvals, if named.isEmpty then Set(policy.owners) else named)
    ChangeVerdict(ChangeId(v.change), decision, v.clauses, (v.reasoning :: bad).mkString("; "), v.confident && bad.isEmpty)

  val instructions: IO[String, String] = Model.prompt("evaluate-policy")

  def make(bedrock: Bedrock): IO[AgentError, PolicyEvaluator] =
    instructions.mapError(AgentError.Config(_)).flatMap(text => CachedEvaluator.make(EvaluatePolicy(bedrock, text)))

/** Keeps verdicts per (policy text, change facts), so re-planning the same change doesn't re-roll the model. The time
  * of the facts only counts by its UTC date, for policies about dates. Failures aren't kept.
  */
final class CachedEvaluator private (underlying: PolicyEvaluator, cache: Ref[Map[String, List[ChangeVerdict]]], limit: Int)
    extends PolicyEvaluator:
  def evaluate(policy: PolicyConfig, facts: List[ChangeFacts]): IO[String, List[ChangeVerdict]] =
    val key = CachedEvaluator.key(policy, facts)
    cache.get.map(_.get(key)).someOrElseZIO:
      underlying.evaluate(policy, facts).tap(v => cache.update(m => (if m.size >= limit then Map.empty else m) + (key -> v)))

object CachedEvaluator:
  def make(underlying: PolicyEvaluator, limit: Int = 1000): UIO[CachedEvaluator] =
    Ref.make(Map.empty[String, List[ChangeVerdict]]).map(CachedEvaluator(underlying, _, limit))

  private val factsCodec = JsonCodec.jsonEncoder(Schema[List[ChangeFacts]])

  def key(policy: PolicyConfig, facts: List[ChangeFacts]): String =
    val day = facts.map(f => f.copy(at = f.at.atOffset(ZoneOffset.UTC).nn.toLocalDate.nn.atStartOfDay(ZoneOffset.UTC).nn.toInstant.nn))
    val digest = MessageDigest.getInstance("SHA-256").nn
    digest.update(policy.toString.getBytes(UTF_8))
    digest.update(factsCodec.encodeJson(day, None).toString.getBytes(UTF_8))
    digest.digest().nn.map(b => f"$b%02x").mkString
