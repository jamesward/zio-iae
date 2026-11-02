package zio.iae.agent.harness

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import zio.*
import zio.http.*
import zio.iae.agent.TestRepo
import zio.iae.webhook.{Signatures, Webhook, WebhookSecret}
import zio.schema.{Schema, derived}
import zio.schema.codec.JsonCodec

final case class ReactAction(ts: String, user: String, reaction: String) derives Schema
final case class ReplyAction(thread: String, user: String, text: String) derives Schema
final case class PushAction(files: Map[String, String], message: String) derives Schema
final case class SpeedAction(speed: Double) derives Schema
final case class ScenarioAction(name: String) derives Schema
final case class ScenarioInfo(name: String, description: String, steps: List[String]) derives Schema
final case class AdvanceAction(minutes: Long) derives Schema
final case class FaultAction(env: String, fault: Fault, on: Boolean) derives Schema
final case class HarnessState(
    users: List[FakeUser],
    channel: String,
    messages: List[FakeMessage],
    files: Map[String, String],
    commits: List[String],
    clock: ClockState,
    scenarios: List[ScenarioInfo],
    run: Option[ScenarioRun],
    envs: List[String],
    faults: List[ActiveFault],
) derives Schema

/** The harness's web UI: the Slack channel (as anyone), and a GitHub panel that edits files and pushes. A push sends
  * a signed GitHub push event through the real webhook, like GitHub would.
  */
final case class HarnessUi(
    slack: FakeSlack,
    repo: TestRepo,
    webhook: Webhook,
    githubSecret: WebhookSecret,
    editable: List[String],
    clock: HarnessClock,
    harness: UIO[Harness],
    run: Ref[Option[ScenarioRun]],
):
  val routes: Routes[Any, Nothing] = Routes(
    Method.GET / ""         -> handler(Response(body = Body.fromString(HarnessPage.html), headers = Headers(Header.ContentType(MediaType.text.html)))),
    Method.GET / "ui" / "state" -> handler(state.map(s => Response.json(JsonCodec.jsonEncoder(Schema[HarnessState]).encodeJson(s, None).toString)).orElseSucceed(Response.status(Status.InternalServerError))),
    Method.POST / "ui" / "react" -> handler((req: Request) => act[ReactAction](req)(a => slack.react(a.ts, a.user, a.reaction))),
    Method.POST / "ui" / "reply" -> handler((req: Request) => act[ReplyAction](req)(a => slack.reply(a.thread, a.user, a.text))),
    Method.POST / "ui" / "push"  -> handler((req: Request) => act[PushAction](req)(a => push(a.files, a.message))),
    Method.POST / "ui" / "clock" / "speed"   -> handler((req: Request) => act[SpeedAction](req)(a => clock.setSpeed(a.speed))),
    Method.POST / "ui" / "clock" / "advance" -> handler((req: Request) => act[AdvanceAction](req)(a => advance(a.minutes.minutes))),
    Method.POST / "ui" / "scenario" -> handler((req: Request) => act[ScenarioAction](req)(a => start(a.name))),
    Method.POST / "ui" / "fault"    -> handler((req: Request) => act[FaultAction](req)(a => ZIO.fromEither(zio.iae.EnvName.from(a.env)).mapError(_.toString).flatMap(env => harness.flatMap(_.faults.set(env, a.fault, a.on))))),
  )

  /** Jumps the clock and wakes the reconcilers, since a jump doesn't end their sleep. */
  def advance(by: Duration): IO[String, Unit] = clock.advance(by) *> harness.flatMap(_.agent.signal)

  /** Starts a scenario in the background; one at a time. */
  private def start(name: String): IO[String, Unit] =
    for
      scenario <- ZIO.fromOption(Scenario.all.find(_.name == name)).orElseFail(s"no scenario $name")
      busy     <- run.get.map(_.exists(_.status == RunStatus.Running))
      _        <- ZIO.fail("a scenario is already running").when(busy)
      h        <- harness
      _        <- Scenario.run(h, scenario, r => run.set(Some(r))).ignoreLogged.forkDaemon
    yield ()

  def push(files: Map[String, String], message: String): IO[String, Unit] =
    for
      sha     <- repo.commit(files, message)
      body     = s"""{"ref":"refs/heads/main","after":"${sha.value}","repository":{"full_name":"local/harness"}}"""
      request  = Request.post(URL.root / "github", Body.fromString(body)).addHeaders(
                   Headers(
                     Header.Custom("x-github-event", "push"),
                     Header.Custom("x-github-delivery", sha.short),
                     Header.Custom("x-hub-signature-256", Signatures.github(githubSecret, body.getBytes(UTF_8))),
                   )
                 )
      response <- webhook.github(request)
      _        <- ZIO.fail(s"webhook answered ${response.status.code}").unless(response.status.isSuccess)
    yield ()

  private def state: IO[String, HarnessState] =
    for
      messages <- slack.messages.get
      files <- ZIO.foreach(editable)(f => ZIO.attemptBlocking(f -> Files.readString(repo.authorClone.resolve(f), UTF_8).nn).mapError(_.getMessage))
      log   <- zio.iae.agent.Process.run(repo.authorClone, Seq("git", "log", "--format=%h %s", "-n", "20"), 30.seconds)
      time  <- clock.state
      now   <- run.get
      infos  = Scenario.all.map(s => ScenarioInfo(s.name, s.description, s.steps.map(_.describe)))
      h     <- harness
      active <- h.faults.list
    yield HarnessState(slack.bot :: slack.users, slack.channel, messages.toList, files.toMap, log.linesIterator.toList, time, infos, now, h.envs.map(_.env.value), active)

  private def act[A: Schema](req: Request)(f: A => IO[String, Unit]): UIO[Response] =
    req.body.asString
      .mapError(_.getMessage)
      .flatMap(body => ZIO.fromEither(JsonCodec.jsonDecoder(Schema[A]).decodeJson(body)))
      .flatMap(f)
      .as(Response.ok)
      .catchAll(e => ZIO.logWarning(s"harness action failed: $e").as(Response.text(e).status(Status.BadRequest)))

object HarnessUi:
  def make(
      slack: FakeSlack,
      repo: TestRepo,
      webhook: Webhook,
      githubSecret: WebhookSecret,
      editable: List[String],
      clock: HarnessClock,
      harness: UIO[Harness],
  ): UIO[HarnessUi] =
    Ref.make(Option.empty[ScenarioRun]).map(HarnessUi(slack, repo, webhook, githubSecret, editable, clock, harness, _))
