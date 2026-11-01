package zio.iae.agent

import com.jamesward.zio_bedrock.ConverseConfig
import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId}
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.testkit.ScriptedModel
import zio.json.ast.Json
import zio.test.*

object FixPullRequestSpec extends ZIOSpecDefault:
  def spec = suite("DiagnoseFailure with pull requests")(
    test("the model writes a fix in a scratch clone and opens one pull request; the env's checkout doesn't move"):
      ZIO.scoped:
        for
          repo     <- TestRepo.make
          commit   <- repo.commit(Map(".iae/manifest.json" -> """{"app"""), "break the manifest")
          ws        = repo.workspace(EnvName("dev"))
          _        <- ws.sync.mapError(_.message)
          model    <- ScriptedModel.make.orDie
          bedrock  <- Model.make(ConverseConfig(ApiKey("k"), model.url, ModelId("scripted")))
          pulls    <- LocalPullRequests.make(repo.remote.toString)
          diagnose <- DiagnoseFailure.make(bedrock, Some(pulls)).mapError(_.message)
          _        <- model.reply(
                        ScriptedModel.Turn.ToolUse("writeFile", Json.Obj("path" -> Json.Str(".iae/manifest.json"), "content" -> Json.Str("""{"app":"photo-app","resources":[],"grants":[]}"""))),
                        ScriptedModel.Turn.ToolUse("openPullRequest", Json.Obj("title" -> Json.Str("Fix the manifest"), "body" -> Json.Str("It was cut short."))),
                        ScriptedModel.Turn.ToolUse("openPullRequest", Json.Obj("title" -> Json.Str("Again"), "body" -> Json.Str("Twice?"))),
                        ScriptedModel.Turn.Text("The manifest was cut short; local://pull/1 fixes it."),
                      )
          text     <- diagnose.diagnose(ws, commit, EnvName("dev"), "Unexpected end of input")
          opened   <- pulls.opened.get
          branch    = opened.head._2
          pushed   <- Process.run(repo.root, Seq("git", "--git-dir", repo.remote.toString, "show", s"$branch:.iae/manifest.json"), 30.seconds)
          head     <- ws.git("rev-parse", "HEAD").mapError(_.message)
          local    <- ws.git("show", "HEAD:.iae/manifest.json").mapError(_.message)
          last     <- model.requests.map(_.last.toString)
        yield assertTrue(
          text.contains("local://pull/1"),
          opened.size == 1,
          branch.startsWith(s"iae/fix-${commit.short}-"),
          opened.head._3 == "Fix the manifest",
          opened.head._4.contains(commit.value),
          pushed.contains("photo-app"),
          head == commit.value,
          local == """{"app""",
          last.contains("already open"),
        ),
  ).provide(Client.default.orDie) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
