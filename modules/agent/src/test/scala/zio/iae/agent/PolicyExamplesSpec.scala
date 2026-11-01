package zio.iae.agent

import java.nio.file.Files
import java.time.Instant
import zio.*
import zio.iae.*
import zio.iae.engine.policy.*
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec
import zio.test.*

object PolicyExamplesSpec extends ZIOSpecDefault:
  val owners    = SlackGroup.from("platform-owners").toOption.get
  val approvers = SlackGroup.from("infra-approvers").toOption.get
  val strict    = PolicyConfig("High risk needs an approver.", owners, Set(owners, approvers), Set(EnvName("prod")))
  val relaxed   = strict.copy(text = "Anything goes.")

  /** Approves high risk under the strict policy; lets everything through under the relaxed one. */
  object StubEvaluator extends PolicyEvaluator:
    def evaluate(policy: PolicyConfig, facts: List[ChangeFacts]): IO[String, List[ChangeVerdict]] =
      ZIO.succeed(facts.map: f =>
        val decision = if policy.text == strict.text && f.risk == Risk.High then Decision.Approve(1, Set(approvers)) else Decision.Auto
        ChangeVerdict(f.id, decision, List(policy.text), "stub", confident = true))

  def facts(risk: Risk): ChangeFacts =
    val address = Address(AppName("photo-app"), EnvName("staging"), ResourceKind.Bucket, LogicalId("uploads"))
    ChangeFacts(ChangeId("x"), EnvName("staging"), Subject.Resource(address, "Update", Nil), risk, Set.empty, Origin.Human, None, Instant.EPOCH.nn)

  final case class ExampleJson(facts: ChangeFacts, expected: Decision) derives Schema

  def exampleJson(risk: Risk, expected: Decision): String =
    JsonCodec.jsonEncoder(Schema[ExampleJson]).encodeJson(ExampleJson(facts(risk), expected), None).toString

  def spec = suite("PolicyExamples")(
    test("examples load from the repo, run under both policies and report failures and changes first"):
      for
        repo <- ZIO.attemptBlocking(Files.createTempDirectory("examples").nn)
        dir   = Files.createDirectories(repo.resolve(PolicyExamples.dir)).nn
        _ <- ZIO.attemptBlocking:
               Files.writeString(dir.resolve("high-needs-approver.json"), exampleJson(Risk.High, Decision.Approve(1, Set(approvers))))
               Files.writeString(dir.resolve("low-is-fine.json"), exampleJson(Risk.Low, Decision.Auto))
               Files.writeString(dir.resolve("notes.txt"), "ignored")
        examples <- PolicyExamples.load(repo)
        results  <- PolicyExamples.run(StubEvaluator, strict, relaxed, examples)
        report    = PolicyExamples.report(results)
      yield assertTrue(
        examples.map(_.name) == List("high-needs-approver", "low-is-fine"),
        results.map(r => (r.example.name, r.passes, r.changed)) == List(("high-needs-approver", false, true), ("low-is-fine", true, false)),
        report.linesIterator.toList(1).startsWith(":x: `high-needs-approver`: can go ahead, expected needs 1 approval(s) from @infra-approvers"),
        report.contains("*(changed: was needs 1 approval(s) from @infra-approvers)*"),
        report.contains("1 of 2 as expected"),
      ),
    test("no folder, no examples"):
      for
        repo     <- ZIO.attemptBlocking(Files.createTempDirectory("examples").nn)
        examples <- PolicyExamples.load(repo)
      yield assertTrue(examples.isEmpty, PolicyExamples.report(Nil).contains("No policy examples")),
    test("a bad example file is a config error naming the file"):
      for
        repo  <- ZIO.attemptBlocking(Files.createTempDirectory("examples").nn)
        _     <- ZIO.attemptBlocking(Files.writeString(Files.createDirectories(repo.resolve(PolicyExamples.dir)).nn.resolve("bad.json"), "{}"))
        error <- PolicyExamples.load(repo).flip
      yield assertTrue(error.message.contains("bad.json")),
  )
