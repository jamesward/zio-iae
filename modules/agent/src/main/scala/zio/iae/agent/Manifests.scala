package zio.iae.agent

import java.nio.file.Path
import zio.*
import zio.iae.*

/** Produces the manifests of a checked-out commit. */
trait ManifestSource:
  def manifests(dir: Path, config: RepoConfig): IO[AgentError, List[Manifest]]

/** Runs each app's manifest command in the workspace. In production the command runs a built app; the local harness
  * and tests can point it at anything that prints a manifest.
  */
final case class CommandManifests(timeout: Duration = 10.minutes) extends ManifestSource:
  def manifests(dir: Path, config: RepoConfig): IO[AgentError, List[Manifest]] =
    ZIO.foreach(config.apps): app =>
      for
        _        <- ZIO.fail(AgentError.Manifest(app.name.value, "no manifest command")).when(app.manifest.isEmpty)
        out      <- Process.run(dir, app.manifest, timeout).mapError(AgentError.Manifest(app.name.value, _))
        manifest <- ZIO.fromEither(Manifest.fromJson(out)).mapError(AgentError.Manifest(app.name.value, _))
        _        <- ZIO.fail(AgentError.Manifest(app.name.value, s"the command printed the manifest of ${manifest.app.value}")).when(manifest.app != app.name)
      yield manifest
