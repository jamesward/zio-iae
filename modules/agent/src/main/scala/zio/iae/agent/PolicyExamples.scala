package zio.iae.agent

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import zio.*
import zio.iae.engine.PlanText
import zio.iae.engine.policy.*
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

/** An example change and the decision the policy should reach for it, from `.iae/policy-examples/<name>.json`. */
final case class PolicyExample(name: String, facts: ChangeFacts, expected: Decision)

/** How the policy behaves on an example before and after a policy change. */
final case class ExampleResult(example: PolicyExample, before: Decision, after: Decision):
  def passes: Boolean  = after == example.expected
  def changed: Boolean = before != after

object PolicyExamples:
  val dir = ".iae/policy-examples"

  private final case class ExampleFile(facts: ChangeFacts, expected: Decision) derives Schema

  private val decoder = JsonCodec.jsonDecoder(Schema[ExampleFile])

  def load(repo: Path): IO[AgentError, List[PolicyExample]] =
    val folder = repo.resolve(dir).nn
    for
      files <- ZIO
                 .attemptBlocking:
                   if !Files.isDirectory(folder) then Nil
                   else Files.list(folder).nn.iterator.nn.asScala.toList.filter(_.toString.endsWith(".json")).sortBy(_.toString)
                 .mapError(e => AgentError.Config(s"listing $dir: ${e.getMessage}"))
      examples <- ZIO.foreach(files): file =>
                    val name = file.getFileName.toString.stripSuffix(".json")
                    ZIO.attemptBlocking(Files.readString(file, UTF_8).nn).mapError(e => AgentError.Config(s"reading $file: ${e.getMessage}"))
                      .flatMap(json => ZIO.fromEither(decoder.decodeJson(json)).mapError(e => AgentError.Config(s"$dir/$name.json: $e")))
                      .map(f => PolicyExample(name, f.facts, f.expected))
    yield examples

  /** Evaluates the examples under both policies, as `Verdicts.decide` would decide them in a plan. Each example's
    * change id is its name, so the evaluator sees unique ids. A failed evaluation decides like a missing verdict.
    */
  def run(evaluator: PolicyEvaluator, before: PolicyConfig, after: PolicyConfig, examples: List[PolicyExample]): UIO[List[ExampleResult]] =
    val facts = examples.map(e => e.facts.copy(id = ChangeId(e.name)))
    def decide(policy: PolicyConfig): UIO[List[Decision]] =
      evaluator.evaluate(policy, facts).orElseSucceed(Nil).map(v => Verdicts.decide(facts, v, policy).changes.map(_.decision))
    if examples.isEmpty then ZIO.succeed(Nil)
    else decide(before).zipWith(decide(after))((b, a) => examples.zip(b.zip(a)).map { case (e, (b, a)) => ExampleResult(e, b, a) })

  /** The results for people: failures and changed decisions first, each marked. */
  def report(results: List[ExampleResult]): String =
    if results.isEmpty then s"No policy examples in `$dir`."
    else
      val lines = results.sortBy(r => (r.passes, !r.changed, r.example.name)).map: r =>
        val mark    = if r.passes then ":white_check_mark:" else ":x:"
        val outcome = if r.passes then PlanText.describe(r.after) else s"${PlanText.describe(r.after)}, expected ${PlanText.describe(r.example.expected)}"
        val change  = if r.changed then s" *(changed: was ${PlanText.describe(r.before)})*" else ""
        s"$mark `${r.example.name}`: $outcome$change"
      val failed = results.count(!_.passes)
      (s"*Policy examples under the new policy:* ${results.size - failed} of ${results.size} as expected" :: lines).mkString("\n")
