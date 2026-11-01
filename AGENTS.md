# zio-iae

Infrastructure as Effects: a Scala 3 / ZIO library whose resource traits (`Bucket`, `Queue`, ...) are the
basis for ZIO layers, plus an always-on agent that turns git pushes into planned, policy-gated AWS changes
and deployments. The design is in [`SPEC.md`](SPEC.md); read it before working.

## Skills

- Follow the `zen-of-projects` Skill. This file records only project facts and exceptions.
- For Scala 3 and ZIO code (writing or reviewing it), follow `zen-of-scala`.
- For design, domain modeling, effects and testing strategy, follow `zen-of-james`.
- Get them with `./sbta extractSkillsJars` (into the gitignored `.kiro/skills/`) and read the relevant ones
  before working.

## MCP

- MCP server `sbta` (this build's `./sbta --mcp`), configured in `.mcp.json` and `.kiro/settings/mcp.json` and
  approved in `.claude/settings.json`. Its tools are deferred in Claude Code: load them with ToolSearch
  (search `sbta`).

## Build, Test & Dev Workflow

- Use the MCP server `sbta` (this build's `./sbta --mcp`) for all sbt work: build, test and run
  with `compile`, `test` (`tests` for specific classes, `full` for all), `run`, `clean` and
  `publish-local`; inspect the build with `projects`, `test-names`, `main-classes`, `dependencies` and
  `show`. Use `sbt-task` for any other command (e.g. `reload`) and `list-tasks` to find tasks. Don't run
  `sbt` or `./sbta` in a shell while the MCP tools are available; if the server is unavailable, say so and
  fall back to `./sbta <command>`.
- After editing Scala 3 sources, check them with `check` (pass the edited files: a fast type check
  without compiling; `"scope":"module"` after changing an API), then run `compile` and `test` before
  declaring the work done.
- For Scala symbols use `glob-search` (find or list), `inspect` (members and signatures) and
  `symbol-location` (source location) rather than text search or guessing APIs. JavaDoc/ScalaDoc
  lookups are the proxied javadocs.dev tools of the same server.

## Modules

| Project | Directory | Kind | Scala |
|---|---|---|---|
| `core` (`iae-core`) | `modules/core` | published library | LTS |
| `awsClient` (`iae-aws-client`) | `modules/aws-client` | published library | LTS |
| `aws` (`iae-aws`) | `modules/aws` | published library | LTS |
| `testkit` (`iae-testkit`) | `modules/testkit` | published library | LTS |
| `engine` (`iae-engine`) | `modules/engine` | published library | LTS |
| `cli` (`iae-cli`) | `modules/cli` | application (`JavaAppPackaging`) | Next |
| `agent` (`iae-agent`) | `modules/agent` | server (`JavaAppPackaging`) | Next |
| `webhook` (`iae-webhook`) | `modules/webhook` | Lambda | Next |
| `photoApp` (`photo-app`) | `examples/photo-app` | example `InfraApp` (`JavaAppPackaging`) | Next |

The library modules set `scalaVersion` to the Scala LTS (bare setting in `build.sbt`); the application
modules override it with `scalaNext`. Applications are `publish / skip`.

## Commands

- Full validation: `./sbta shutdown`, `./sbta extractSkillsJars`, then `./sbta "Test / compile; testFull; stage"`
  (`stage` covers the applications and the example). CI runs the same command.
- Docker: required. The `aws` and `photoApp` tests start Floci (`FlociLayer` in `iae-testkit`) through
  Testcontainers.
- Run the example against a local Floci (`docker run --rm -p 4566:4566 floci/floci:2.4.0`), after `photoApp/stage`:
  `AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_ENDPOINT_URL=http://localhost:4566
  IAE_ENV=dev IAE_PROVISION=auto target/out/jvm/scala-3.10.0/photo-app/universal/stage/bin/photo-app`; add
  `--iae-manifest` to print the manifest instead.
- Gated tests (none yet; planned in `SPEC.md` §4): real-AWS contract tests behind `IAE_AWS_CONTRACT_TESTS=1`,
  real-model policy evals, and the nightly sandbox smoke test. All are paid and stay out of the default
  validation.

## Maintenance routine

- `.factory/MAINTENANCE.md`, weekly once the factory schedules it.

## Exceptions

- **Floci image (`floci/floci:2.4.0` in `FlociLayer.image`):** Floci emulates the AWS APIs, which have no
  production version to track, so the maintenance routine bumps it to the latest Floci release like a
  dependency (unlike images that mirror a production service).
