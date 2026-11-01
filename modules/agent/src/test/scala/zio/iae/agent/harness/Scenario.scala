package zio.iae.agent.harness

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import zio.*
import zio.iae.*
import zio.iae.aws.BucketNaming
import zio.schema.{Schema, derived}

/** One step of a scenario. Steps act like the people and systems around the agent; waits watch Slack for what the
  * agent says. A wait only matches messages posted after the previous wait's match.
  */
enum Step:
  case Push(description: String, message: String, files: Harness => IO[String, Map[String, String]])
  /** A push whose webhook delivery is lost; only the periodic reconcile can notice it. */
  case PushWithoutWebhook(description: String, message: String, files: Harness => IO[String, Map[String, String]])
  /** A reaction on the newest approval request. */
  case React(user: String, reaction: String)
  case WaitFor(description: String, matches: (Harness, String) => Boolean)
  case Advance(by: Duration)
  case RestartAgent
  /** A change made outside the agent, e.g. by hand in the console. */
  case Drift(description: String, change: Harness => IO[String, Unit])

  def describe: String = this match
    case Push(d, _, _)               => s"push: $d"
    case PushWithoutWebhook(d, _, _) => s"push without the webhook: $d"
    case React(user, reaction)       => s"$user reacts :$reaction: on the newest approval request"
    case WaitFor(d, _)               => s"wait for $d"
    case Advance(by)                 => s"advance the clock by ${by.render}"
    case RestartAgent                => "restart the agent"
    case Drift(d, _)                 => s"drift: $d"

final case class Scenario(name: String, description: String, steps: List[Step])

@zio.schema.annotation.simpleEnum
enum RunStatus derives Schema, CanEqual:
  case Running, Passed, Failed

final case class ScenarioRun(name: String, step: Int, total: Int, current: String, status: RunStatus, error: Option[String]) derives Schema

object Scenario:
  /** Runs the steps in order, reporting progress; fails with the step that didn't happen. */
  def run(harness: Harness, scenario: Scenario, progress: ScenarioRun => UIO[Unit], timeout: Duration = 2.minutes): IO[String, Unit] =
    def report(i: Int, status: RunStatus, error: Option[String] = None) =
      progress(ScenarioRun(scenario.name, i + 1, scenario.steps.size, scenario.steps.lift(i).fold("done")(_.describe), status, error))
    for
      cursor <- harness.slack.messages.get.map(_.size).flatMap(Ref.make)
      _ <- ZIO.foreachDiscard(scenario.steps.zipWithIndex): (step, i) =>
             (report(i, RunStatus.Running) *> perform(harness, step, cursor, timeout))
               .tapError(e => report(i, RunStatus.Failed, Some(e)))
               .mapError(e => s"${scenario.name}, step ${i + 1} (${step.describe}): $e")
      _ <- report(scenario.steps.size, RunStatus.Passed)
    yield ()

  private def perform(harness: Harness, step: Step, cursor: Ref[Int], timeout: Duration): IO[String, Unit] =
    step match
      case Step.Push(_, message, files)               => files(harness).flatMap(harness.ui.push(_, message))
      case Step.PushWithoutWebhook(_, message, files) => files(harness).flatMap(harness.repo.commit(_, message)).unit
      case Step.React(user, reaction) =>
        harness.slack.messages.get.map(_.findLast(_.request.isDefined)).someOrFail("there is no approval request")
          .flatMap(m => harness.slack.react(m.ts, user, reaction))
      case Step.WaitFor(description, matches) =>
        val found = for
          from     <- cursor.get
          messages <- harness.slack.messages.get
          index     = messages.indexWhere(m => matches(harness, m.text), from)
          _        <- cursor.set(index + 1).when(index >= 0)
        yield Option.when(index >= 0)(())
        found
          .repeat(Schedule.spaced(250.millis) && Schedule.recurUntil[Option[Unit]](_.isDefined))
          .withClock(Clock.ClockLive)
          .timeoutFail(s"$description didn't happen within ${timeout.render}")(timeout)
          .withClock(Clock.ClockLive)
          .unit
      case Step.Advance(by)       => harness.ui.advance(by)
      case Step.RestartAgent      => harness.agent.restart
      case Step.Drift(_, change)  => change(harness)

  // The built-in scenarios, for the UI's buttons and the headless tests.

  object UploadsVersioned extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))

  private val plain     = Bucket.readWrite(Harness.Uploads).manifest(Harness.app).toOption.get.toJson
  private val versioned = Bucket.readWrite(UploadsVersioned).manifest(Harness.app).toOption.get.toJson

  private def current(harness: Harness, path: String): IO[String, String] =
    ZIO.attemptBlocking(Files.readString(harness.repo.authorClone.resolve(path), UTF_8).nn).mapError(_.getMessage)

  /** Turns the uploads bucket's versioning on if it's off, and off if it's on: an Update, medium risk. */
  val toggleVersioning: Harness => IO[String, Map[String, String]] = harness =>
    current(harness, Harness.manifest).map(m => Map(Harness.manifest -> (if m.trim == versioned.trim then plain else versioned)))

  private def env(harness: Harness, index: Int): String = harness.envs(index).env.value

  private def approvalIn(index: Int): Step = Step.WaitFor(
    s"an approval request for env ${index + 1}",
    (h, text) => text.startsWith("*Approval needed*") && text.contains(s"${Harness.app.value}/${env(h, index)}"),
  )

  private def appliedIn(index: Int, by: Option[String] = None): Step = Step.WaitFor(
    s"the change applied in env ${index + 1}${by.fold("")(u => s", approved by $u")}",
    (h, text) => text.startsWith("Applied") && text.contains(s"${Harness.app.value}/${env(h, index)}") && by.forall(u => text.contains(s"<@$u>")),
  )

  val gatedChange = Scenario(
    "gated-change",
    "A prod change that needs an approver: people who may not approve are ignored, then alice approves.",
    List(
      Step.Push("toggle the uploads bucket's versioning", "toggle versioning", toggleVersioning),
      appliedIn(0),
      approvalIn(1),
      Step.React("U_CAROL", "white_check_mark"),
      Step.React("U_DEV", "white_check_mark"),
      Step.React("U_ALICE", "white_check_mark"),
      appliedIn(1, Some("U_ALICE")),
    ),
  )

  val rejectedChange = Scenario(
    "rejected-change",
    "An approver rejects a prod change; the agent stops and says so once.",
    List(
      Step.Push("toggle the uploads bucket's versioning", "toggle versioning", toggleVersioning),
      approvalIn(1),
      Step.React("U_ALICE", "x"),
      Step.WaitFor("the agent to stop", (h, text) => text.startsWith("Not applying") && text.contains("rejected it")),
    ),
  )

  val nobodyAnswers = Scenario(
    "nobody-answers",
    "Nobody answers an approval request for a day: it expires and the agent asks again.",
    List(
      Step.Push("toggle the uploads bucket's versioning", "toggle versioning", toggleVersioning),
      approvalIn(1),
      Step.Advance(25.hours),
      approvalIn(1),
    ),
  )

  val droppedEvent = Scenario(
    "dropped-event",
    "A push whose webhook never arrives is picked up by the periodic reconcile.",
    List(
      Step.PushWithoutWebhook("toggle the uploads bucket's versioning", "toggle versioning, quietly", toggleVersioning),
      Step.Advance(16.minutes),
      appliedIn(0),
    ),
  )

  val agentRestart = Scenario(
    "agent-restart",
    "The agent restarts while an approval is open; it finds the request in Slack and applies once approved.",
    List(
      Step.Push("toggle the uploads bucket's versioning", "toggle versioning", toggleVersioning),
      approvalIn(1),
      Step.RestartAgent,
      Step.React("U_ALICE", "white_check_mark"),
      appliedIn(1, Some("U_ALICE")),
    ),
  )

  val drift = Scenario(
    "drift",
    "Someone changes the staging bucket's versioning by hand; the next periodic reconcile puts it back.",
    List(
      Step.Drift(
        "flip the staging bucket's versioning through the S3 API",
        harness =>
          for
            manifest <- current(harness, Harness.manifest)
            bucket   <- ZIO.fromEither(BucketNaming(None).physical(Address(Harness.app, harness.envs.head.env, ResourceKind.Bucket, Harness.Uploads.logicalId)))
                          .mapError(_.message)
            _        <- harness.s3.head.putBucketVersioning(bucket, enabled = manifest.trim != versioned.trim).mapError(_.toString)
          yield (),
      ),
      Step.Advance(16.minutes),
      appliedIn(0),
    ),
  )

  val policyChange = Scenario(
    "policy-change",
    "A push that changes POLICY.md needs an owner, even for a change the old policy lets through.",
    List(
      Step.Push(
        "toggle versioning and loosen the policy",
        "toggle versioning; let everything through",
        harness =>
          for
            change <- toggleVersioning(harness)
            policy <- current(harness, ".iae/POLICY.md")
          yield change + (".iae/POLICY.md" -> (policy + "\n- Everything is fine without asking.\n")),
      ),
      Step.WaitFor(
        "an owners' approval request in env 1",
        (h, text) => text.startsWith("*Approval needed*") && text.contains(s"${Harness.app.value}/${env(h, 0)}") && text.contains("@platform-owners"),
      ),
      Step.React("U_BOB", "white_check_mark"),
      Step.React("U_ALICE", "white_check_mark"),
      appliedIn(0, Some("U_BOB")),
    ),
  )

  val buildFailure = Scenario(
    "build-failure",
    "A push whose build fails is announced once (with the model's diagnosis in the thread when one is configured).",
    List(
      Step.Push("break the manifest", "break the manifest", _ => ZIO.succeed(Map(Harness.manifest -> """{"app"""))),
      Step.WaitFor("the failure notice for env 1", (h, text) => text.startsWith("*Failed*") && text.contains(s"`${env(h, 0)}`")),
    ),
  )

  val all: List[Scenario] = List(gatedChange, rejectedChange, nobodyAnswers, droppedEvent, agentRestart, drift, policyChange, buildFailure)
