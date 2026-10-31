# zio-iae: Infrastructure as Effects

Status: **Draft v0.1** (2026-10-31)

zio-iae makes cloud infrastructure part of a ZIO program's type. An app that needs an S3 bucket asks for one in its
environment (`ZIO[Bucket[Uploads], ...]`) the same way it asks for any other service. The infrastructure is
declared once and used for three things:

1. **At runtime** it provides the ZIO layers the program uses (`Bucket`, `Queue`, ...).
2. **At deploy time** it is the desired-state description that an engine reconciles against AWS.
3. **For security** it generates the app's least-privilege IAM policy from the capabilities the program uses.

A Claude Code scheduled task (routine) runs the pipeline. It reacts to changes on `main`, plans and applies
infrastructure changes, deploys the app (later with canaries), gets human approval where policy requires it, and
records evidence in AWS Audit Manager. Later the same agent takes production alerts, triages them and proposes
fixes, which then go through the same pipeline.

The project has two parts:

| Part | What it is | Who uses it |
|---|---|---|
| **Library** (`iae-core`, `iae-aws`, `iae-aws-client`, `iae-testkit`) | Resource traits, the infra description model, AWS providers, a thin ZIO AWS client, Floci test layers | App developers, in their ZIO programs |
| **Agent** (`iae-engine`, `iae-agent`, routine prompt) | Deterministic plan/apply/deploy engine exposed as a CLI and MCP server, plus the Claude Code routine that drives it | The routine; humans approving |

---

## 1. Goals and non-goals

### Goals

- **Infra is a typed effect requirement.** Using a resource the app hasn't declared is a compile error, and a
  declared resource that is never used shows up as a warning in the plan.
- **One declaration, three uses:** runtime layer, desired state, IAM policy.
- **Capability-scoped traits** (`BucketReader` vs `Bucket`) that map directly to IAM actions.
- **Agent-managed delivery.** Every merge to `main` converges each environment to `main` HEAD with no human
  steps unless policy calls for an approval.
- **Humans approve, the LLM doesn't.** The model orchestrates, explains and fixes. It never holds approval
  authority, and every mutation goes through the deterministic engine, which enforces policy.
- **Auditable by construction.** Every change links commit, tests, plan, approval, apply result and deploy
  verification, and is recorded as AWS Audit Manager evidence.
- **Local-first testing.** The whole stack, from the app's use of `Bucket` up to the engine's plan/apply, runs
  against [Floci](https://github.com/floci-io/floci) through Testcontainers.

### Non-goals (for now)

- Clouds other than AWS. The core model must not assume AWS, but only AWS providers ship.
- Replacing Terraform/CDK for arbitrary infra. zio-iae manages the resources apps use, plus the platform pieces
  it needs to deploy them.
- Importing existing hand-made resources. That can come later through an explicit `adopt` command.
- A UI. GitHub (PRs, deployments, statuses) and Audit Manager are the UI.

---

## 2. Concepts

| Term | Meaning |
|---|---|
| **Resource** | A unit of managed infra, such as a bucket, queue, table, secret or service. Has a kind, a logical id and a typed spec. |
| **Capability** | A typed view of a resource that the app uses, for example `BucketReader`, `BucketWriter`, `QueueSender`. Determines the IAM grant. |
| **Infra[R]** | A composable, inspectable description of resources plus the capabilities that produce a ZIO environment `R`. |
| **Manifest** | The serialized `Infra` of an app at a commit: resources, specs, capabilities, dependencies and the IAM grants derived from them. Pure data with no AWS calls. |
| **Environment (env)** | A deployment target such as `dev` (Floci), `staging` or `prod`. One AWS account + region per env. |
| **Observed state** | What the provider reads from AWS for a resource address. |
| **Plan** | An ordered DAG of `Change`s that takes observed state to the manifest, with a risk level per change. Content-addressed by a hash. |
| **Policy** | Rules mapping (env, change, risk) to `Auto`, `Approve(n)` or `Deny`. |
| **Approval** | A human decision bound to one plan hash, env, and expiry. |
| **Engine** | The deterministic `iae` program that does manifest, plan, apply, deploy, verify and evidence. It's the only thing that mutates AWS. |
| **Agent** | Claude in a scheduled task (routine) that calls the engine, explains plans, opens PRs, diagnoses failures, and later triages alerts. |

---

## 3. Library

### 3.1 Modules

| Module | Depends on | Contents |
|---|---|---|
| `iae-core` | zio, zio-streams, zio-json (or zio-schema) | Resource/Capability/Infra model, `ResourceId`, `Spec`, `Change`, `Plan`, `Risk`, `Policy`, `Provider` SPI, `InfraApp`, manifest codecs, IAM policy model. **No AWS.** |
| `iae-aws-client` | zio, zio-http | Thin ZIO-native AWS clients: SigV4 signing, credentials chain, retries, and the operations we need for S3, SQS, DynamoDB, Secrets Manager, IAM, STS, ECR, ECS, ELBv2, CloudWatch, CloudTrail, Audit Manager. Endpoint override for Floci. |
| `iae-aws` | `iae-core`, `iae-aws-client` | AWS implementations: runtime capability layers (`Bucket` backed by S3, ...) and `Provider`s (observe/diff/apply) per resource kind, naming, tagging, IAM generation. |
| `iae-testkit` | `iae-aws`, `testcontainers-floci` | `FlociLayer`, `AwsConfig` for Floci, in-memory fakes for services Floci doesn't emulate (Audit Manager), test helpers (`assertConverges`). |

Library modules use the Scala LTS (per the `zen-of-projects` compatibility rules). Package: `zio.iae` (see open
question Q1).

### 3.2 Resource identity

ZIO environments are keyed by type, so two buckets need two types. Every resource is identified by a singleton
object that carries its logical id and spec:

```scala
object Uploads extends Bucket.Id("uploads", BucketSpec(versioning = true))
object Thumbs  extends Bucket.Id("thumbs",  BucketSpec(lifecycle = List(Expire(after = 30.days))))
object Jobs    extends Queue.Id("jobs",     QueueSpec(visibilityTimeout = 30.seconds, dlq = Some(Dlq(maxReceives = 5))))
```

The capability types are parameterised by the id's singleton type: `Bucket[Uploads.type]`. A type alias keeps
signatures readable (`type Uploads = Uploads.type`). It still has to be verified that `izumi-reflect` `Tag`s
for singleton object types are stable across compilation units (see open question Q2). The fallback is user-declared
marker traits.

Logical ids are stable. Renaming an id is a replace (delete + create) and the plan shows it as such, unless the
id declares `formerly = "old-id"`, which turns it into a move.

### 3.3 Capability traits

Each resource kind has a full trait and narrower capability traits. The engine derives IAM actions from the
narrowest capability the app requests.

```scala
trait BucketReader[Id]:
  def get(key: ObjectKey): ZStream[Any, BucketError, Byte]
  def head(key: ObjectKey): IO[BucketError, Option[ObjectMeta]]
  def list(prefix: ObjectKey = ObjectKey.root): ZStream[Any, BucketError, ObjectMeta]
  def presignGet(key: ObjectKey, ttl: Duration): IO[BucketError, URL]

trait BucketWriter[Id]:
  def put(key: ObjectKey, body: ZStream[Any, Throwable, Byte], meta: PutMeta = PutMeta.default): IO[BucketError, Unit]
  def delete(key: ObjectKey): IO[BucketError, Unit]
  def presignPut(key: ObjectKey, ttl: Duration): IO[BucketError, URL]

trait Bucket[Id] extends BucketReader[Id], BucketWriter[Id]

enum BucketError:
  case NotFound(key: ObjectKey)
  case AccessDenied(detail: String)
  case PreconditionFailed(detail: String)
  case Transport(cause: Throwable)   // retried by the client before surfacing
```

Errors are typed and specific to the resource. Transport errors are retried with a `Schedule` inside the client
and only surface when retries run out.

MVP resource kinds and capabilities:

| Kind | AWS | Capabilities | Stateful (retain by default) |
|---|---|---|---|
| `Bucket` | S3 | `BucketReader`, `BucketWriter`, `Bucket` | yes |
| `Queue` | SQS (+DLQ) | `QueueSender`, `QueueReceiver`, `Queue` | yes (messages) |
| `Table` | DynamoDB (key-value, typed by a zio-schema `Schema[A]`) | `TableReader`, `TableWriter`, `Table` | yes |
| `Secret` | Secrets Manager | `SecretReader` (writes are out-of-band) | yes |
| `Topic` | SNS | `TopicPublisher`, subscriptions declared in infra | no |

Phase 2: `Service` (container on ECS Fargate behind an ALB, image in ECR). It is a deployable rather than a
capability: the app *is* the service. See §5.6.

### 3.4 Declaring infra: `Infra[R]`

`Infra[R]` is a static, inspectable value. It isn't a `ZLayer`, because layers are opaque and can't be listed
without running them. It can be turned into one:

```scala
final case class Infra[+R](
  resources: Chunk[ResourceDecl],          // id, kind, spec, dependencies
  grants:    Chunk[Grant],                 // capability -> IAM actions on resource
  layer:     ZLayer[Cloud, InfraError, R]  // runtime binding
):
  def ++[R2](that: Infra[R2]): Infra[R & R2]
  def manifest(app: AppId): Manifest
```

Constructors live on each resource kind:

```scala
Bucket.reader(Uploads)   : Infra[BucketReader[Uploads]]
Bucket.readWrite(Uploads): Infra[Bucket[Uploads]]
Queue.sender(Jobs)       : Infra[QueueSender[Jobs]]
Topic.publisher(Events).subscribe(Jobs)  // wiring is part of the declaration
```

Resources that are only wired together (such as a DLQ, or a bucket notification to a queue) are declared without
granting the app a capability on them.

`Cloud` is the runtime context: env name, account, region, naming strategy, endpoint override (Floci), credentials.
It is provided by `Cloud.fromEnvironment` in production and `FlociLayer.cloud` in tests.

At runtime, `layer` **binds, it doesn't create**. It resolves the physical name and checks (cheaply, once at
startup) that the resource exists with the expected `iae:spec-hash` tag. If not, it fails fast with
`InfraError.NotProvisioned`, which means the engine hasn't converged this env yet. A `Cloud.autoProvision` mode
for `dev` creates missing resources directly, so a local run against Floci needs no engine.

### 3.5 `InfraApp`

```scala
object PhotoApp extends InfraApp:
  val infra = Bucket.readWrite(Uploads) ++ Bucket.readWrite(Thumbs) ++ Queue.sender(Jobs)

  def run: ZIO[InfraEnv, Any, Any] =   // InfraEnv is inferred from infra's R
    for
      _ <- ZIO.serviceWithZIO[Bucket[Uploads]](_.put(key, body))
      _ <- ZIO.serviceWithZIO[QueueSender[Jobs]](_.send(Job.Resize(key)))
    yield ()
```

`InfraApp` derives `type InfraEnv` from `infra`, so `run` can't require a resource that isn't declared (a compile
error). Its `main`:

- `--iae-manifest` writes the manifest JSON to stdout and exits without contacting AWS. The engine uses this to
  get the desired state at a commit.
- Otherwise it builds `infra.layer` from `Cloud.fromEnvironment` and runs `run`.

Apps that aren't a single `main` (a library module of shared infra, or several services in one repo) extend
`InfraModule` (just `val infra`), and the repo's `.iae/config.json` lists the entry points.

### 3.6 Naming, tagging, ownership

- Physical name: `<app>-<env>-<id>`, plus a short stable hash suffix where AWS needs global uniqueness (S3).
- Every managed resource is tagged `iae:app`, `iae:env`, `iae:id`, `iae:kind`, `iae:spec-hash`,
  `iae:commit`.
- **Tags are the ownership record.** There is no separate state file to drift from reality. The engine
  discovers what it owns through the Resource Groups Tagging API and per-service list calls.
- IAM policies for the deploy role are conditioned on `aws:ResourceTag/iae:managed = true`, so the engine
  can't touch resources it doesn't own.

### 3.7 IAM generation

From `Infra.grants` the engine generates one runtime role per app per env, with one statement per (capability,
resource), for example `BucketReader[Uploads]` gives `s3:GetObject` and `s3:ListBucket` on that bucket only.
The role is itself a managed resource, so widening it is a plan change with its own risk class (see §5.3).

### 3.8 Provider SPI

```scala
trait Provider[S <: Spec]:
  type Observed
  def kind: ResourceKind
  def observe(addr: Address): ZIO[AwsClients, ProviderError, Option[Observed]]
  def diff(desired: S, observed: Option[Observed]): Chunk[Change]    // pure
  def apply(change: Change): ZIO[AwsClients, ProviderError, Observed]
  def risk(change: Change, env: Env): Risk                          // pure, default by change type
```

- `diff` is pure and total, and is property-tested.
- `apply` is idempotent. Re-running an applied change is a no-op, which is needed for resuming after a crash.
- `Change` is `Create | Update(fields) | Replace(reason) | Delete | Move(from)`, and every `Update` lists the
  attribute-level diff for display.

### 3.9 AWS client (`iae-aws-client`)

We build thin, hand-written, ZIO-native clients instead of wrapping the Java SDK:

- `zio-http` transport, SigV4 signer, credentials chain (env, web identity, container/instance metadata,
  `AssumeRole`), region resolution, `AWS_ENDPOINT_URL` override (this is how we point at Floci).
- Protocols: REST-XML (S3), JSON 1.0/1.1 (DynamoDB, SQS, Secrets Manager, ECR, ECS, CloudTrail, CloudWatch Logs),
  REST-JSON (Audit Manager), Query (IAM, STS, ELBv2, CloudWatch, SNS).
- Only the operations providers and capabilities need, each as a ZIO method with typed errors that map AWS error
  codes to ADTs. Streaming bodies as `ZStream[Any, E, Byte]`.
- Retries with jittered exponential backoff on throttling and 5xx, as a `Schedule`, overridable per call.
- Every op is tested against Floci except Audit Manager, which has a fake plus gated real-AWS contract tests.

Alternative: generated clients ([zio-aws](https://github.com/zio/zio-aws)) for the long tail. See open question Q3.

---

## 4. Testing

| Level | What | Backend |
|---|---|---|
| Unit | `diff`, `risk`, policy, plan ordering, manifest codec round trip, IAM generation | pure, property-based (zio-test `Gen`) |
| Client | Each AWS op | Floci |
| Provider | create, then observe, then diff is empty; update; replace; delete; `apply` idempotency | Floci |
| Capability | `Bucket[Id]` etc. used by a sample program | Floci with `Cloud.autoProvision` |
| Engine end-to-end | manifest, plan, apply, re-plan gives an empty plan (`assertConverges`); crash mid-apply then resume; lock contention | Floci + in-memory state fakes |
| Audit Manager | evidence import | in-memory fake; real AWS gated (`IAE_AWS_CONTRACT_TESTS=1`) |
| Real AWS smoke | One sandbox account, nightly | AWS (gated, paid) |

`iae-testkit` provides:

```scala
object FlociLayer:
  val container: ZLayer[Any, Throwable, FlociContainer]   // ZLayer.scoped around io.floci:testcontainers-floci
  val awsConfig: ZLayer[FlociContainer, Nothing, AwsConfig]
  val cloud:     ZLayer[FlociContainer, Nothing, Cloud]    // env = "test", autoProvision on
```

One container per suite (`provideShared`). Each test gets a fresh `env` name so tests don't collide. Floci's
emulated services include S3, SQS, SNS, DynamoDB, IAM, STS, KMS, Secrets Manager, ECS, ECR, ELBv2, CloudWatch,
CloudTrail and Config. **Audit Manager isn't emulated**, so it needs the fake. Floci emulates; it isn't AWS, so
provider behaviour that matters (IAM evaluation, S3 consistency edge cases) also gets real-AWS smoke coverage.

---

## 5. Agent and engine

### 5.1 Split of responsibilities

| Engine (`iae`, deterministic Scala) | Agent (Claude in a routine) |
|---|---|
| Build manifest at a commit | Decide which envs to reconcile; run the engine |
| Observe, plan, classify risk, evaluate policy | Explain the plan in plain language in the plan PR |
| Acquire locks, apply, journal, resume | Open, update and close plan PRs |
| Verify approvals (GitHub API), bind them to the plan hash | Diagnose failed builds, tests, applies and canaries; open fix PRs (never push to `main`) |
| Deploy, canary, auto-rollback on alarm | Summarise outcomes; later: triage alerts and propose fixes |
| Write evidence to S3 + Audit Manager | |
| **Only component holding AWS write credentials** | **Never calls mutating AWS APIs directly** |

The engine is a published application (Scala Next) with two faces:

- CLI: `iae manifest | plan | apply | deploy | promote | rollback | drift | status | bootstrap`
- MCP server: `iae --mcp`, the same operations as structured tools that return JSON (plan with risks, policy
  verdict, apply journal), so the agent doesn't parse logs.

The routine's environment setup script installs the engine (for example with coursier) and registers the MCP
server. The routine's prompt lives in this repo (`agent/ROUTINE.md`) and tells the agent which tools to use and
in what order.

### 5.2 Triggers

Scheduled tasks support schedule, API (`/fire` with a free-text payload) and GitHub triggers. The GitHub trigger
covers **pull request, release and issue events, not `push`**. Each event starts its own session, and events
above an hourly cap are dropped. So:

| Trigger | Mechanism | Purpose |
|---|---|---|
| **Merge to `main`** (primary) | GitHub trigger `pull_request.closed`, filter *is merged = true*, *base branch = main* | The "git push" event. `main` is protected with a ruleset (no direct pushes, no bypass), so every change arrives this way. |
| Direct push (fallback) | `.github/workflows/iae-fire.yml` on `push` to `main` that POSTs `{"text": "<repo> <sha>"}` to the routine's `/fire` | Only if direct pushes are allowed. |
| Reconcile safety net | Schedule, hourly | Catches dropped events, finishes interrupted runs, drift detection. |
| Alerts (phase 3) | CloudWatch alarm, SNS, a small Lambda, `/fire` with the alarm summary | Triage, see §7. |

**Runs are level-triggered.** An event is only a wake-up. Every run reconciles each env toward the *current*
`main` HEAD, not the event's sha. Dropped, duplicated or out-of-order events are harmless, and concurrent sessions
serialise on a per-env lock. Fire payloads arrive as untrusted text, so the routine prompt uses them only as hints
and re-derives everything from git and AWS.

### 5.3 Risk and policy

Risk classes, assigned by providers with these defaults:

| Risk | Examples |
|---|---|
| `Low` | Create; tag-only update; app deploy with no infra change |
| `Medium` | In-place config update (visibility timeout, lifecycle rules); IAM narrowing |
| `High` | Replace of a stateless resource; IAM widening; public-access, encryption or network change; new prod service |
| `Critical` | Delete or replace of a stateful resource; anything touching data retention |

Default policy, overridable in code (`.iae/Policy.scala`, typed, evaluated by the engine at the commit being
deployed):

| Env | Low | Medium | High | Critical |
|---|---|---|---|---|
| dev / test | auto | auto | auto | auto |
| staging | auto | auto | approve(1) | approve(1) |
| prod | auto | approve(1) | approve(1) | approve(2) + `allowDestroy` on the resource |

A stateful resource removed from the manifest isn't deleted by default. It's **orphaned**: tagged
`iae:orphaned=<date>`, left alone, and reported. Deletion requires the resource to be declared with
`Retention.Delete` (or an explicit `iae destroy <id>`), and is then `Critical`.

### 5.4 Human-in-the-loop approvals

**Mechanism: the plan PR.** It uses only GitHub primitives, works with the routine's event model, and leaves an
audit trail.

1. The engine computes a plan for an env, and policy returns `Approve(n)`.
2. The agent pushes branch `claude/iae-plan/<env>/<plan-hash-prefix>` with one file,
   `.iae/approvals/<env>/<plan-hash>.json` (env, base commit, plan hash, the observed-state hash it was computed
   from, the change list, expiry), and opens a PR titled `[iae] <env>: <summary>`. The body has the agent's
   plain-language explanation, the full change table with risks, the blast radius, and the rollback story. If an
   older open plan PR exists for the env, the agent closes it as superseded.
3. CODEOWNERS for `.iae/approvals/**` names the approver team, and the `main` ruleset requires their review
   (no bypass). **Approving the PR and merging it is the approval.** Requested changes or closing it is a
   rejection.
4. The merge is itself a merge to `main`, so it triggers a run (§5.2). The reconciler recomputes the plan. If the
   hash matches an approval file on `main` whose PR the engine has verified, the engine applies.

Engine-side checks before applying an approved plan. The engine doesn't trust the agent for any of these:

- The recomputed plan hash equals the approved hash. If anything drifted, the plan is stale and a new plan PR
  is opened.
- Through the GitHub API (engine's own read-only token): the PR that added the file was merged into `main`, has
  ≥ n approving reviews from members of the approver team, and no approver is the PR author or the agent's
  identity.
- Not expired (default 24h for prod).
- The approval file is never reused. After applying, the engine records `applied` in the state table, keyed by
  plan hash.

Alternatives considered:

- *GitHub Environments with required reviewers*: good UX and native, but only gates GitHub Actions jobs. It fits
  if applies move into Actions (see §5.8, option B).
- *Label or `/approve` comment on an issue*: needs an issue trigger and is weaker (any writer can label). Fine
  for `staging`, too weak for `prod`.
- *Slack approve button* (later, through a connector): nicer for on-call, but the approval record would live
  outside GitHub. It could create the approving PR review on the human's behalf.

Break-glass: a human with admin AWS access can run `iae apply --break-glass --reason ...` locally. The engine
records it as an exception in Audit Manager.

### 5.5 Pipeline for one reconcile run

```
for env in [staging, prod]:              # promotion order from .iae/config.json
  lock(env)                              # DynamoDB conditional write with lease; skip if held
  resume any incomplete apply journal for env
  sha      = main HEAD
  if env != first and sha not yet verified in previous env: continue      # promotion gate
  build + test at sha                    # sbt Test/compile; test (Floci); result cached by sha
  manifest = iae manifest @ sha
  plan     = observe + diff (expand phase only: creates/updates, no deletes)
  verdict  = policy(env, plan)
    Deny    -> report, stop env
    Approve -> ensure plan PR open; stop env (resumes on merge)
    Auto or approved ->
      apply(plan)                        # journaled, idempotent
      deploy(app image @ sha)            # phase 2, §5.6
      verify                             # health + alarms over a bake window
      contract phase: deletes/orphaning (a separate plan; usually Approve in prod)
  evidence(env, sha, plan, approval, results)
  GitHub Deployment status for (env, sha)
  unlock(env)
```

**Expand/contract ordering** keeps old and new app versions working during rollout: additive changes go before
the deploy and removals go after it. A failure at any step stops that env, rolls back the deploy if needed (infra
expand changes are left in place since they're additive), records evidence, and makes the agent post a diagnosis
on the merged PR or open a fix PR.

### 5.6 Deploy and canary (phase 2)

- The build produces a container image (sbt-native-packager Docker) pushed to the env's ECR, tagged with the sha.
- `Service` resource: ECS Fargate service with **two target groups** behind an ALB listener with weighted
  forwarding.
- Canary: start the new task set on the green target group, shift weight `0 → 10 → 50 → 100%` with a bake time at
  each step. CloudWatch alarms (5xx rate, latency p99, plus app-declared alarms in `Infra`) gate each step. If an
  alarm fires, weight goes back to 0 and the deploy is marked failed. All of this is done by the engine, not the
  agent.
- Staging gets the same flow with shorter bake times, so the canary machinery runs on every change.

### 5.7 State

Each env account has a small, engine-owned DynamoDB table `iae-state` (created by `iae bootstrap`):

| Item | Key | Purpose |
|---|---|---|
| Lock | `lock#<env>` | Lease with owner session id and expiry; conditional writes |
| Plan record | `plan#<hash>` | Plan, verdict, approval ref, status (`pending/applying/applied/failed`) |
| Apply journal | `journal#<hash>#<step>` | Per-change progress for crash-safe resume |
| Deployment | `deploy#<env>#<sha>` | Canary steps, verification result |

This table holds no resource state. That stays in AWS tags (§3.6).

### 5.8 Credentials and trust boundary

Routine sessions are ephemeral cloud containers with env vars set per environment, so whatever credentials the
engine uses are visible to anyone who can use that environment.

- **Option A (MVP):** the routine environment holds credentials for an `iae-deployer` role per env. The role can
  only touch `iae:managed` resources and has a permissions boundary; it has no IAM wildcard. The engine enforces
  policy and approvals. Risk: a compromised session could bypass the engine with the same credentials.
- **Option B (target):** the routine holds only an `iae-planner` read role plus permission to start an **in-account
  apply executor** (Step Functions or CodeBuild running the same engine). The executor re-verifies the approval
  against GitHub and holds the write role. The agent can then propose and observe but can't mutate on its own. A
  variant runs the executor in GitHub Actions with OIDC and Environments.

We start with A against non-prod accounts and move to B before managing prod. See open question Q5.

---

## 6. Audit Manager integration

`iae bootstrap` creates, per env account:

- A custom framework **"IAE Change Management"** with custom controls (manual evidence sources):
  - `IAE-CM-1` change originates from a reviewed commit on `main`
  - `IAE-CM-2` automated tests passed for that commit
  - `IAE-CM-3` plan reviewed and approved as required by policy (or auto-approved by policy version X)
  - `IAE-CM-4` deployment verified (health and canary results)
  - `IAE-CM-5` exceptions: break-glass, rollbacks, failed applies
- An assessment over that framework scoped to the env account, plus optionally a standard framework (such as SOC 2)
  so CloudTrail/Config automated evidence about the same resources sits next to ours.
- An evidence bucket `iae-evidence-<env>` with Object Lock (compliance mode) and versioning.

For each reconcile of an env, the engine writes one JSON evidence document per control to the evidence bucket
(commit, PR + reviewers, test summary, plan hash + changes, approval refs, apply journal, CloudTrail event IDs,
canary results) and imports it with `BatchImportEvidenceToAssessmentControl` (`s3ResourcePath`). Evidence is
batched per run because manual evidence is limited to 100 uploads per control per day.

Audit Manager has no Floci emulation. `iae-testkit` provides `AuditManagerFake` (records imports, enforces the
daily limit), and contract tests run against a real account when gated on.

---

## 7. Later: monitors and triage (phase 3)

- An alert fires the routine through `/fire` with the alarm summary.
- The agent gets read-only investigation tools from the engine's MCP server (`iae status`, recent deployments,
  CloudWatch logs and metrics queries scoped to the app) plus the repo.
- Possible outcomes:
  1. **Recent deploy correlates:** the engine runs `iae rollback <env>`. Policy can allow auto-rollback in prod,
     since it returns to an already-approved state.
  2. **Code fix:** the agent opens a PR with a fix and a regression test, linking the alert. It goes through
     review and the normal pipeline, with canary.
  3. **Infra tuning** (such as queue visibility or concurrency): a PR changing the `Infra` declaration, so the change
     is reviewed like any other.
  4. **Unknown:** the agent writes the triage notes to an issue and pages a human.
- The agent never mutates prod outside the pipeline. Rollback is the only fast path, and it's policy-gated.

---

## 8. Repository layout

```
zio-iae/
  build.sbt                    # sbt 2 multi-project (bootstrapped via start.jamesward.com)
  AGENTS.md                    # project facts; follows zen-of-projects
  SPEC.md                      # this document
  modules/
    core/        aws-client/   aws/        testkit/      # library (Scala LTS)
    engine/      agent/                                  # application (Scala Next): CLI + MCP server
  agent/
    ROUTINE.md                 # the scheduled task's prompt
    setup.sh                   # environment setup script: installs iae, registers MCP
  examples/
    photo-app/                 # InfraApp using Bucket + Queue; end-to-end on Floci
  .github/workflows/           # CI (build + Floci tests); optional iae-fire.yml
```

A repo using zio-iae has:

```
.iae/config.json               # app entry points, envs + accounts/regions, promotion order
.iae/Policy.scala              # optional policy overrides
.iae/approvals/<env>/*.json    # approval records (written by plan PRs)
CODEOWNERS                     # approver team for .iae/approvals/**
```

---

## 9. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | Project bootstrap via start.jamesward.com (`AGENTS.md`, sbt 2, CI, sbta) | CI green on an empty module set |
| M1 | `iae-core` model; `iae-aws-client` SigV4 + S3; `Bucket` capability + provider; `FlociLayer`; manifest | Example app reads and writes a bucket on Floci; provider convergence tests pass |
| M2 | Engine: plan, policy, apply, journal, lock, tag discovery; `Queue`, `Table`, `Secret`, `Topic`; IAM generation | `assertConverges` end-to-end on Floci for the example app, including crash/resume |
| M3 | Routine + MCP server; merge trigger; plan PRs and engine-side approval verification; GitHub Deployments; staging account | A merged PR that changes infra converges staging; a High-risk change waits for the plan PR |
| M4 | Audit Manager bootstrap + evidence; prod account; credentials option B | Prod change with approval produces evidence for IAE-CM-1..4 |
| M5 | `Service` + ECR + ALB canary + auto-rollback | Bad build in staging is rolled back automatically by an alarm |
| M6 | Alert trigger + triage tools | Synthetic alarm results in a rollback or a fix PR |

---

## 10. Open questions

| # | Question | Current leaning |
|---|---|---|
| Q1 | Package and artifact naming: `zio.iae` / `dev.zio`-style vs `com.jamesward` | `zio.iae` package, `com.jamesward` org, unless it moves under the ZIO org |
| Q2 | Resource identity via singleton-type `Tag`s: verify izumi-reflect stability; fallback to marker traits | Spike in M1 |
| Q3 | Hand-written `iae-aws-client` vs zio-aws vs Java SDK v2 | Hand-written for the ~40 ops we need (ZIO-native streams, Scala 3, small footprint); revisit if the surface grows |
| Q4 | Manifest extraction: running `main --iae-manifest` via sbt per commit is slow | Cache by sha; later an sbt task that only links the `infra` value |
| Q5 | Move to credentials option B before prod? Executor in Step Functions/CodeBuild vs GitHub Actions + OIDC | B before prod; pick executor in M4 |
| Q6 | Engine's GitHub token for approval verification (routine sessions reach GitHub through MCP; the engine needs its own read-only token) | Fine-grained read-only token in the env, or verification moves into the executor (B) |
| Q7 | Multi-app repos and shared infra (VPC, ALB, DNS zone) owned by a platform `InfraModule` | Platform module with its own plan PRs; apps reference it by id |
| Q8 | Multi-region / multi-account per env | Out of scope until M5 |
| Q9 | Scheduled-task event caps and research-preview churn | Level-triggered design + hourly reconcile makes dropped events harmless; keep the routine prompt thin so the engine holds the logic |

---

## 11. Next step

Bootstrap the repository with the instructions at <https://start.jamesward.com> (the `zen-of-projects` Skill):
sbt 2, `AGENTS.md`, CI, sbta. Then lay out the modules in §8 and start M1.
