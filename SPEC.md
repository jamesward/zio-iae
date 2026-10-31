# zio-iae: Infrastructure as Effects

Status: **Draft v0.2** (2026-10-31)

zio-iae makes cloud infrastructure part of a ZIO program's type. An app that needs an S3 bucket asks for one in its
environment (`ZIO[Bucket[Uploads], ...]`) the same way it asks for any other service. The infrastructure is
declared once and used for three things:

1. **At runtime** it provides the ZIO layers the program uses (`Bucket`, `Queue`, ...).
2. **At deploy time** it is the desired-state description that an engine reconciles against AWS.
3. **For security** it generates the app's least-privilege IAM policy from the capabilities the program uses.

An always-on, stateful agent process runs the pipeline. It receives git pushes through its own GitHub webhook,
plans and applies infrastructure changes (including changes it authors itself), deploys the app with canaries, and
records evidence in AWS Audit Manager. The same process runs monitors, drift detection, resource cleanup and cost
optimisation. Deterministic code makes the decisions that can be made by code; an LLM loop built on
[zio-bedrock](https://github.com/jamesward/zio-bedrock) handles judgement, conversation and code changes. **The user configures a policy** that says which kinds of changes need a human
approval. The agent talks to people in **Slack**: it asks for approvals there, asks for guidance when it's unsure,
and reports what it did. When production alerts fire, it triages them and proposes fixes, which go through the
same pipeline.

The project has two parts:

| Part | What it is | Who uses it |
|---|---|---|
| **Library** (`iae-core`, `iae-aws`, `iae-aws-client`, `iae-testkit`) | Resource traits, the infra description model, AWS providers, a thin ZIO AWS client, Floci test layers | App developers, in their ZIO programs |
| **Agent** (`iae-engine`, `iae-cli`, `iae-agent`, `iae-webhook`) | Deterministic plan/apply/deploy engine, the `iae` CLI, and the always-on agent process (inbox, reconcilers, background jobs, Slack, zio-bedrock LLM tasks) | Runs in a tooling AWS account; humans approve and guide it in Slack |

---

## 1. Goals and non-goals

### Goals

- **Infra is a typed effect requirement.** Using a resource the app hasn't declared is a compile error, and a
  declared resource that is never used shows up as a warning in the plan.
- **One declaration, three uses:** runtime layer, desired state, IAM policy.
- **Capability-scoped traits** (`BucketReader` vs `Bucket`) that map directly to IAM actions.
- **Agent-managed delivery.** Every push to `main` converges each environment to `main` HEAD with no human
  steps unless policy calls for an approval.
- **An always-on, stateful agent.** One long-running process handles pushes, approvals, conversations, monitors,
  drift, cleanup and cost. Its in-memory state is a working set it can rebuild from git, AWS and Slack.
- **The agent can change infra; the user's policy decides when a human must approve.** Policy is code in the
  user's repo, matched against each concrete change (env, resource, change type, risk, who authored it). The
  agent never holds approval authority, and every mutation goes through the deterministic engine, which enforces
  the policy.
- **Slack is the human channel.** Approval requests, questions to humans, and status notices all happen in Slack
  threads. Approvals are reactions by people in a configured Slack user group, verified by the engine through
  Slack's API.
- **Auditable by construction.** Every change links commit, tests, plan, approval, apply result and deploy
  verification, and is recorded as AWS Audit Manager evidence.
- **No state file.** State is what is provisioned, read through the cloud's own APIs. The engine keeps no
  table, file or database of its own to decide what to do (§5.3).
- **Local-first testing.** The whole stack, from the app's use of `Bucket` up to the engine's plan/apply, runs
  against [Floci](https://github.com/floci-io/floci) through Testcontainers.

### Non-goals (for now)

- Clouds other than AWS. The core model must not assume AWS, but only AWS providers ship.
- Replacing Terraform/CDK for arbitrary infra. zio-iae manages the resources apps use, plus the platform pieces
  it needs to deploy them.
- Importing existing hand-made resources. That can come later through an explicit `adopt` command.
- A UI. Slack (conversation, approvals), GitHub (PRs, deployments) and Audit Manager are the UI.

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
| **Policy** | User-written rules in the repo that map each change (env, resource, change type, risk, origin) to `Auto`, `Approve(n, group)` or `Deny`. |
| **Approval** | Reactions from enough members of the required Slack group on an engine-posted approval request, bound to one env, target manifest and change set, with an expiry. |
| **Run thread** | The Slack thread for one (app, env, target manifest). All approvals, questions and notices for that rollout go there. |
| **Engine** | The deterministic library (used by the agent and the `iae` CLI) that does manifest, plan, policy, apply, deploy, verify and evidence. It's the only path that mutates AWS. |
| **Agent** | The always-on `iae-agent` process: inbox, per-env reconcilers, background jobs, Slack, and LLM tasks built on zio-bedrock. |
| **Task** | One bounded LLM job inside the agent (explain a plan, diagnose a failure, triage an alert, ...), run as a zio-bedrock loop with a fixed tool set and budget. |

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
capability: the app *is* the service. See §5.9.

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
- **Tags are the ownership record.** There is no state file, so nothing can drift from reality. The engine
  discovers what it owns through the Resource Groups Tagging API and per-service list calls. `iae:commit` is
  also the concurrency guard (§5.3).
- IAM policies for the deploy role are conditioned on `aws:ResourceTag/iae:managed = true`, so the engine
  can't touch resources it doesn't own.

### 3.7 IAM generation

From `Infra.grants` the engine generates one runtime role per app per env, with one statement per (capability,
resource), for example `BucketReader[Uploads]` gives `s3:GetObject` and `s3:ListBucket` on that bucket only.
The role is itself a managed resource, so widening it is a plan change with its own risk class (see §5.5).

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
| Engine end-to-end | manifest, plan, apply, re-plan gives an empty plan (`assertConverges`); crash mid-apply then re-run converges; an older commit can't overwrite a newer one (commit guard) | Floci |
| Agent | Inbox routing and coalescing, reconciler flow, approval verification, restart rehydration, job candidate detection | Floci + `SlackFake` + webhook fixtures |
| LLM tasks | Tool dispatch, budgets, safety rules (no mutation without policy), prompt-injection fixtures | `ScriptedModel` (zio-bedrock custom origin) |
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

## 5. The agent

### 5.1 Architecture

The agent is an **always-on, stateful Scala/ZIO process** (`iae-agent`) running in a dedicated tooling AWS
account. It contains the deterministic engine as a library and uses
[zio-bedrock](https://github.com/jamesward/zio-bedrock) for the LLM parts. There is no Claude Code routine: we
receive git pushes through our own GitHub webhook, and the long-running jobs (reconcilers, monitors, cleanup, cost
optimizer) are fibers in the same process.

```
 GitHub ──webhook──▶ iae-webhook (Lambda, verifies signature) ──▶ SQS inbox ─┐
 CloudWatch alarms / Config / CloudTrail / Cost Anomaly ──EventBridge──▶ SQS ─┤
 Slack ◀──Socket Mode (outbound websocket)──────────────────────────────────┐ │
                                                                            ▼ ▼
                         ┌──────────────────── iae-agent (ECS Fargate, 1 task) ─────────────────────┐
                         │ Inbox router ─▶ per-env reconciler fibers ─▶ engine (plan/policy/apply)  │
                         │             ─▶ background jobs (monitor, drift, cleanup, cost)            │
                         │             ─▶ LLM tasks (zio-bedrock loop) ─▶ tools (read, ask, PR, ...) │
                         │ In-memory state: Ref/Queue/Hub; git workspaces on local disk             │
                         └───────┬──────────────────────┬──────────────────────┬───────────────────┘
                                 │ assume planner role  │ start builds         │ Bedrock (Claude)
                                 ▼                      ▼                      ▼
                         env accounts (staging, prod)  CodeBuild iae-build   Converse API
```

Split of responsibilities inside the process:

| Deterministic (engine + router + jobs) | LLM (zio-bedrock tasks) |
|---|---|
| Receive and route events; decide what to reconcile | Explain plans in plain language in the run thread |
| Build manifest, observe, plan, classify risk, evaluate the user's policy | Diagnose failed builds, tests, applies and canaries |
| Post approval requests; verify approvals through Slack's API | Converse in Slack threads: answer questions, ask for guidance |
| Apply idempotently with the commit guard; deploy; canary; auto-rollback | Author changes (code, `Infra` declarations, config) as PRs |
| Detect drift, orphans, idle resources, cost anomalies | Triage alerts; judge cleanup and cost recommendations |
| Write evidence to S3 + Audit Manager | |
| **Only path to AWS mutations** | **Only gets tools; no raw AWS or shell access** |

Rule of thumb: anything that can be decided by code is decided by code. The LLM is called for judgement, language
and code changes, and everything it wants to change goes back through the deterministic path and the policy.

Humans can use the same engine from a laptop through the `iae` CLI (`plan`, `policy explain`, `status`,
`apply --break-glass`). An MCP server mode (`iae --mcp`) is kept for people who want to drive it from Claude
Code, but the agent doesn't need it.

### 5.2 Inputs

| Input | Mechanism | Handled by |
|---|---|---|
| **Push to `main`** | GitHub App webhook (`push`), received by `iae-webhook`, a small Lambda with a Function URL that verifies `X-Hub-Signature-256` and enqueues to the SQS inbox (dedupe by `X-GitHub-Delivery`) | Reconciler for every env of the affected apps |
| Pull requests | Same webhook (`pull_request`) | Plan preview + policy status check `iae/policy` on the PR; agent-authored PR flow (§5.5) |
| Slack | Socket Mode: the agent opens an outbound websocket, so there's no public Slack endpoint | Approvals (§5.6), conversations (§5.7) |
| Alarms | CloudWatch alarm state changes through EventBridge to the inbox | Monitor job (§5.10) |
| Drift | AWS Config / CloudTrail events on `iae:managed` resources by a principal other than the deployer role, through EventBridge | Drift job (§5.10) |
| Cost | Cost Anomaly Detection through EventBridge; scheduled Cost Explorer reads | Cost job (§5.10) |
| Build results | CodeBuild state changes through EventBridge | Reconciler waiting on that build |
| Timers | `Schedule`s in the process: periodic full reconcile (default 15 min), cleanup (daily), cost review (daily, weekly report), reminders and expiries | The owning job |

The agent has **no inbound network endpoint** of its own. GitHub reaches it through the webhook Lambda and SQS;
Slack through a websocket the agent opens. SQS keeps events while the agent restarts or redeploys (up to 14
days), and the periodic reconcile covers anything that's lost anyway.

### 5.3 State

The agent is stateful in memory; infra state still lives only in the cloud.

**What the agent holds** (ZIO `Ref`s, `Queue`s, `Hub`s, and local disk):

- One **reconciler fiber per env**, with a mailbox. Reconciles of an env are serial, and pending triggers
  coalesce: ten pushes while a reconcile runs cause one follow-up reconcile to the new `main` HEAD.
- Waits: approvals and questions being waited on, with their reminder and expiry timers.
- LLM conversations: message history per Slack thread or task, plus token usage against budgets.
- Builds and deploys in progress (CodeBuild ids, canary step, bake timers).
- Monitor baselines, recent alarms, cost history windows.
- Git workspaces: a checkout per repo on local disk, for manifest builds and agent-authored changes.

**What it never holds:** the source of truth for anything. Infra state is read from the cloud's APIs (tags,
service APIs, CloudTrail), desired state and policy from git, approvals and conversations from Slack. The agent
keeps no state file, table or database. Its in-memory state is a working set that can be thrown away.

**Restart = rehydrate.** On start (or after a crash or its own redeploy) the agent:

1. Reconciles every env from git + AWS, exactly as the periodic reconcile does.
2. Lists its open run threads in Slack (recent iae bot messages) to restore pending approvals and questions, and
   rebuilds each conversation's context from the thread text.
3. Reads open PRs it authored, and in-progress CodeBuild builds and ECS deployments.
4. Drains the SQS inbox, which redelivers anything not yet acknowledged.

An LLM task interrupted by a restart isn't resumed mid-loop. It's restarted from its trigger with the context
rebuilt above. Tasks are written so that re-running them is safe (their side effects are idempotent: same PR
branch, same thread).

Derived facts, as before:

| Need | Derived from |
|---|---|
| What we own | Tags (`iae:app`, `iae:env`, `iae:id`) via the Tagging API and list calls (§3.6) |
| Whether a resource is current | `iae:spec-hash` and `iae:commit` tags on the resource |
| Crash recovery | Nothing to resume: every `apply` is idempotent, so a re-run re-observes and the remaining changes are simply the new plan (covered by the approval as a subset, §5.6) |
| Approvals | Reactions on the engine's approval request, read through Slack's API (§5.6) |
| Approval already used | The recomputed plan is empty once applied |
| What is deployed | ECS service, its task definition and deployments (image tag = sha), ALB listener weights |
| Canary progress | Observed listener weights and ECS deployment rollout state, so an interrupted canary resumes from where the cloud says it is |
| Promotion gate | The previous env's service is observed running the sha, tagged `iae:verified=<sha>` after its bake window passed |
| Already built | Image for the sha exists in ECR |
| How long something has been orphaned or idle | `iae:orphaned=<date>` tag; CloudWatch metrics history |
| History | CloudTrail, ECS deployment history, the evidence bucket, transcripts. **Write-only** for decisions: never read back to decide anything. |

**Concurrency.** One agent task runs at a time and each env has one reconciler fiber, so applies are serialised
without a lock. The **commit guard** stays as a safety net for the brief overlap while the agent itself is
redeployed: before changing a resource, the engine re-reads its `iae:commit` tag and stops if a newer commit has
already touched it, so an older process can never roll back a newer one.

### 5.4 LLM tasks with zio-bedrock

The agent uses zio-bedrock's `Bedrock.loop` (typed multi-turn loop with tools derived from `zio.schema.Schema`)
on the Converse backend with a Claude model on Bedrock (`BEDROCK_MODEL_ID`). Each LLM use is a **task**:

| Task | Started by | Tools (beyond the read-only set) | Ends with |
|---|---|---|---|
| `ExplainPlan` | Approval request posted | — | A thread reply explaining the plan |
| `DiagnoseFailure` | Failed build, test, apply or canary | workspace edit, `runBuild`, `openPr`, `ask` | A diagnosis, plus a fix PR or a question |
| `Converse` | Human message in a run thread or a mention of the bot | `ask`, `notify`, `openPr`, `requestReconcile` | A reply |
| `Triage` | Alarm (§7) | `rollback` (policy-gated), workspace edit, `runBuild`, `openPr`, `ask` | Rollback, fix PR, or escalation |
| `ReviewCleanup` | Cleanup job candidates | `proposeCleanup` | Proposals in Slack |
| `ReviewCost` | Cost job findings | workspace edit, `openPr` | A cost report, and PRs for changes worth making |

- **System prompts** live in the repo (`agent/prompts/<task>.md`) so they're versioned and reviewed. Each task has
  a turn limit, a token budget and a timeout; daily budgets per task kind are enforced from zio-bedrock's usage
  metrics.
- **Read-only tools:** `observe`, `plan`, `policyExplain`, `status`, CloudWatch logs and metrics queries scoped to
  the app, Cost Explorer queries, git log and diff, file read and search in a workspace, Slack thread read.
- **Effectful tools** never mutate infra directly: editing files in a local workspace, `runBuild` (CodeBuild on a
  `iae/` branch), `openPr` / `updatePr` through the GitHub App, `ask` / `notify` in Slack, `requestReconcile`.
  The gated tools (`rollback`, `proposeCleanup`) go through the engine's policy evaluation and come back with
  `Done`, `NeedsApproval(requestLink)` or `Denied(rules)`.
- **There is no `apply` tool and no raw AWS or shell tool.** Infra changes reach AWS only by becoming code on
  `main` or a policy-approved engine action.
- **Untrusted input:** Slack messages, PR text, logs, alert payloads and file contents are passed as data in
  tool results, never as instructions. A prompt injection can at worst produce a bad PR or Slack message, which
  still has to pass review and policy.
- **Transcripts:** every task's messages, tool calls and usage go to CloudWatch Logs (`/iae/agent/tasks`), and
  evidence links to them (§6).
- **Tests:** zio-bedrock accepts a custom origin, so `iae-testkit` has a `ScriptedModel` server that replays
  scripted model turns. Task tests are deterministic and check tool dispatch, budgets and the safety rules. For
  local runs against a real model, point it at Bedrock or at [acprock](https://github.com/jamesward/acprock).

### 5.5 Approval policy (user-configured)

The user decides what needs a human. zio-iae ships risk classes and a conservative default, but the policy is code
in the user's repo (`.iae/Policy.scala`), type-checked, unit-testable, and reviewed like any other code.

Risk classes, assigned by providers with these defaults (a provider can raise a specific change's risk; the user
can't lower a built-in `Critical`):

| Risk | Examples |
|---|---|
| `Low` | Create; tag-only update; app deploy with no infra change |
| `Medium` | In-place config update (visibility timeout, lifecycle rules); IAM narrowing |
| `High` | Replace of a stateless resource; IAM widening; public-access, encryption or network change; new prod service |
| `Critical` | Delete or replace of a stateful resource; anything touching data retention |

A policy is a list of rules plus a default. Each rule matches on facts about one change and gives a verdict:

```scala
object Policy extends IaePolicy:
  val approvers = SlackGroup("infra-approvers")   // Slack user group handle
  val owners    = SlackGroup("platform-owners")

  val rules = List(
    rule("dev is free")       .when(env("dev"))                                   .auto,
    rule("no public buckets") .when(kind(Bucket) && publicAccess)                 .deny,
    rule("prod deletes")      .when(env("prod") && changeType(Delete, Replace))   .approve(2, owners),
    rule("iam widening")      .when(iamWidening)                                  .approve(1, approvers),
    rule("agent in prod")     .when(env("prod") && origin(Agent) && risk >= Medium).approve(1, approvers),
    rule("hot tables")        .when(resource("orders"))                           .approve(1, owners),
    rule("holiday freeze")    .when(env("prod") && during(Freeze.holidays))       .approve(1, owners),
  )

  val default = byRisk(
    Low      -> Auto,
    Medium   -> Auto,
    High     -> Approve(1, approvers),
    Critical -> Approve(2, owners),
  )
```

Facts a rule can match: env; resource kind and id; change type (`Create`, `Update(fields)`, `Replace`, `Delete`,
`Move`, `Deploy`, `Rollback`); risk; derived flags (`iamWidening`, `publicAccess`, `encryptionChange`,
`dataLoss`); **origin** (`Human` for commits authored by people, `Agent` for commits the agent authored, `Alert`, `Drift`, `Cleanup` and
`Cost` for changes the background jobs propose, §5.10); time windows.

Evaluation:

- Every change is evaluated. The plan's verdict is the **most restrictive** across all matching rules (and the
  default for changes no rule matches): `Deny` beats `Approve`, a larger `n` beats a smaller one, and the required
  groups are combined. Order doesn't matter, so adding a rule can only tighten things.
- Each verdict carries the rules that produced it, so the approval request can say *why* approval is needed.
- `iae policy explain` evaluates the policy against a plan without applying anything, and `iae-testkit` has
  helpers to unit-test a policy against sample plans.

Guarding the policy itself:

- The policy used for an env is the one at the commit **currently deployed** there (read from the `iae:commit`
  tags, §5.3), not the one being deployed. A change can't loosen its own gate.
- Any change to `.iae/Policy.scala` is itself a change the engine plans (`PolicyChange`), and a built-in rule that
  the user can't override requires `Approve(1, owners)` for it, where `owners` is set at bootstrap.
- The agent may propose policy changes but they never count as `Auto`.
- Changes to the agent's own deployment (§5.12) always need `Approve(1, owners)`, also built in.

Agent-authored changes. The agent can change infra by opening PRs (a fix, a tuning change, a new resource). For an
agent PR the engine previews the plan for each env at the PR head and evaluates policy with `origin = Agent`:

- If every env's verdict is `Auto`, the agent may merge its own PR. This needs a GitHub ruleset that lets the
  agent's identity merge with a required status check `iae/policy` that the engine sets. Human code review
  requirements stay whatever the user configured in GitHub.
- Otherwise the agent asks for approval in Slack before merging. Approvals bind to the manifest and change set,
  not the commit (§5.6), so an approval given on the PR preview still covers the run after merge as long as the
  plan hasn't changed.

A stateful resource removed from the manifest isn't deleted by default. It's **orphaned**: tagged
`iae:orphaned=<date>`, left alone, and reported. Deletion requires the resource to be declared with
`Retention.Delete` (or an explicit `iae destroy <id>`), and is then `Critical`.

### 5.6 Approvals

When a plan's verdict is `Approve(n, groups)`:

1. The engine posts an **approval request** in the run thread for (app, env, target manifest). It is a structured
   Slack message from the iae bot: env, commit and PR, the change table with risks, the policy rules that require
   approval, the required group and count, the expiry, and a footer with the plan hash. If an open request for the
   same plan exists, the engine reuses it. Older requests for the env are edited to say "superseded".
2. The agent replies in the thread with a plain-language explanation: what changes, why, blast radius, how to roll
   back, and anything it's unsure about.
3. **A human approves by reacting ✅** (`:white_check_mark:`) on the request, or rejects with ❌. The agent receives the
   reaction over Slack Socket Mode (§5.7) and re-runs the env's reconcile.
4. The reconcile recomputes the plan and asks the engine whether it's approved.

The engine's checks, made with its own Slack token and never trusting the agent:

- The request message was posted by the iae bot user and its footer carries the plan hash.
- The recomputed plan is covered by the request: same target manifest hash, and its changes are a subset of the
  requested changes. A subset is allowed so a crashed apply can finish without re-approval, since the changes
  already made drop out of the recomputed plan. Anything outside the set (drift, a newer commit) makes the request
  stale and a new one is posted.
- `reactions.get` shows ✅ from at least `n` distinct users who are members of every required group
  (`usergroups.users.list`), are human (not bots), and aren't the change's author (GitHub-to-Slack identity
  mapping in `.iae/config.json`). Any ❌ from a member of a required group rejects it.
- Not expired (default 24h for prod, configurable in policy).
- Reuse is impossible without a record: once applied, the recomputed plan is empty and there's nothing left for
  the approval to cover.

Slack attributes each reaction to the real user who added it. So even though the agent can post through the iae
bot, it can't produce a human approval.

Evidence (§6) captures the request permalink, the approvers' Slack and mapped GitHub identities, reaction times
and the matched rules.

Alternatives considered:

- *Plan PRs* (approval file merged with CODEOWNERS review): strong GitHub-native trail, but slow and awkward for
  on-call. It could come back as an optional second channel for teams that live in GitHub.
- *Slack buttons*: nicer UX, and the agent receives clicks over Socket Mode, but a click isn't recorded anywhere
  in Slack, so the record would have to be a message the agent writes, which the agent could forge. Planned as v2
  with a separate approvals app whose token only the executor (§5.11) holds.
- *GitHub Environments with required reviewers*: only gates GitHub Actions jobs. It fits if applies move into
  Actions.

Break-glass: a human with admin AWS access can run `iae apply --break-glass --reason ...` locally. The engine
posts it to the channel and records it as an exception in Audit Manager.

### 5.7 Slack: the agent's communication channel

**Layout.** One channel per app (`#iae-<app>`, configured in `.iae/config.json`), one **run thread** per
(env, target manifest), and one thread per alert, cleanup proposal or cost report. Everything about one piece of
work happens in its thread, so the thread is the conversation's durable record.

**Message kinds:**

| Kind | Posted by | Content | Expects |
|---|---|---|---|
| Approval request | engine | Structured plan, matched rules, required group/count, expiry, plan hash | ✅ / ❌ reactions |
| Question | agent (`ask` tool) | The situation, the options, the agent's recommendation, and what happens with no answer | A threaded reply |
| Notice | engine or agent | Applied, deployed, canary step, verified, rolled back, failed, drift found, with links | Nothing |
| Report | cost / cleanup jobs | Periodic summary with proposals | Reactions or replies per proposal |
| Escalation | agent | Something that needs someone now; mentions the on-call group | A reply |

**When the agent asks for guidance.** The task prompts tell the agent to ask instead of guessing when: it isn't
confident of a fix; there's more than one reasonable fix with different trade-offs; a change it would author is
outside what the policy lets it do alone; it sees drift it didn't cause; or an alert's cause is unclear. People
can also start a conversation by mentioning the bot in the channel ("why did staging roll back?").

**Waiting.** A question parks the task: the conversation stays in memory and the agent does other work. A reply
arrives over Socket Mode and resumes the task with the reply appended. The default for no answer is always to
do nothing: after a reminder period (default 1h) the agent mentions the group, and when the wait expires it
posts that it stopped. After a restart, waits are restored from the thread (§5.3).

**Guidance isn't authorisation.** A reply can steer what the agent does or proposes ("use a new bucket instead of
renaming it"), but it never authorises an apply. Anything the policy gates still needs ✅ reactions on an engine
approval request. Replies count only from members of the configured responder group, and the task prompts treat
their text as information to weigh, not as instructions that override the policy.

**Identity and tokens.** One Slack app, "iae", with a bot token (post, update, read history and reactions, read
user groups) and an app-level token for Socket Mode, both in Secrets Manager in the tooling account.

**No state.** Conversations and approvals live in Slack and are read back through Slack's API, the same way
infra is read back through AWS's. Slack message retention for the channel has to be at least the longest
approval or question expiry (open question Q11).

### 5.8 Pipeline: one env reconcile

```
# run by the env's reconciler fiber (§5.3), on a push, an approval, a timer or a drift event
for env in [staging, prod]:              # promotion order from .iae/config.json
  sha      = main HEAD
  if env != first and previous env isn't observed running sha verified: continue   # promotion gate (§5.3)
  build + test at sha                    # CodeBuild: sbt Test/compile; test (Floci); image to ECR. Skipped if the image exists
  manifest = iae manifest @ sha
  plan     = observe + diff (expand phase only: creates/updates, no deletes)
  verdict  = policy(env, plan)
    Deny    -> report, stop env
    Approve -> approved? continue : (post or reuse the Slack approval request; wait, resumes on ✅)
    Auto or approved ->
      apply(plan)                        # idempotent, re-observe + commit guard before each change
      deploy(app image @ sha)            # §5.9
      verify                             # health + alarms over a bake window
      contract phase: deletes/orphaning (a separate plan; usually Approve in prod)
  evidence(env, sha, plan, approval, results)
  GitHub Deployment status for (env, sha)   # display only, never read back
```

**Expand/contract ordering** keeps old and new app versions working during rollout: additive changes go before
the deploy and removals go after it. A failure at any step stops that env, rolls back the deploy if needed (infra
expand changes are left in place since they're additive), records evidence, and starts a `DiagnoseFailure` task (§5.4): the agent posts a diagnosis in the run thread
and either proposes a fix PR or asks for guidance.

### 5.9 Deploy and canary

- The build (CodeBuild, §5.8) produces a container image (sbt-native-packager Docker) pushed to ECR, tagged
  with the sha.
- `Service` resource: ECS Fargate service with **two target groups** behind an ALB listener with weighted
  forwarding.
- Canary: start the new task set on the green target group, shift weight `0 → 10 → 50 → 100%` with a bake time at
  each step. CloudWatch alarms (5xx rate, latency p99, plus app-declared alarms in `Infra`) gate each step. If an
  alarm fires, weight goes back to 0 and the deploy is marked failed. All of this is deterministic engine code; the
  canary watcher is a fiber in the agent process, so a bake window is just a `ZIO.sleep` between checks.
- Staging gets the same flow with shorter bake times, so the canary machinery runs on every change.

### 5.10 Background jobs

Each job is a supervised fiber with its own `Schedule` and inbox subscription. Jobs find things
deterministically and hand judgement to an LLM task. Every change they want goes through the policy with the job
as `origin`, so the user decides how much each job may do alone.

**Reconcilers** (§5.8): one per env. Triggered by pushes, approvals, build results and drift, plus a periodic
full reconcile.

**Monitor.** Watches CloudWatch alarms for every managed app: the defaults (5xx rate, latency, queue age, DLQ
depth, throttles) and the alarms the app declares in `Infra`. Alarms open a `Triage` task (§7). The monitor also
does a periodic health sweep and posts a notice when an env degrades without an alarm firing.

**Drift.** AWS Config and CloudTrail events on `iae:managed` resources from a principal other than the deployer
role mean someone changed infra by hand. The agent posts what changed and who did it, then plans a revert to the
manifest. The revert is a normal plan, gated by policy (`origin = Drift`), with the drift described in the
approval request. If the hand change should stay, the answer is a PR changing the `Infra` declaration.

**Cleanup.** Daily. Candidates:

- orphaned resources (`iae:orphaned=<date>`) older than the retention window in policy;
- resources tagged for an app or env that no longer exists in any repo's manifest;
- idle resources: no requests or messages for N days, from CloudWatch metrics;
- old ECR images and task definitions beyond the last N per app and env, never one that's deployed or the
  rollback target.

`ReviewCleanup` groups them and adds context ("bucket still has 12 GB, last written 94 days ago"). Each
proposal is a plan with `origin = Cleanup`: deleting stateful resources is `Critical`, so it needs approvals by
default, while old images and task definitions can be `Auto` if the user's policy allows.

**Cost optimizer.** Daily data collection, weekly report:

- Cost Explorer by the `iae:app` and `iae:env` cost-allocation tags (activated by `iae bootstrap`);
- Cost Anomaly Detection events (immediate notice);
- Compute Optimizer recommendations and CloudWatch utilisation for services;
- storage patterns (S3 access, DynamoDB capacity mode fit).

`ReviewCost` decides which findings are worth acting on. Because desired state is code, every optimisation is a
**PR to the `Infra` declaration** (right-size a `Service`, switch a table's capacity mode, add a lifecycle
rule), with the expected savings in the description. It then goes through the normal pipeline and policy
(`origin = Cost`).

### 5.11 Credentials and trust boundary

The agent runs in its own **tooling account** on ECS Fargate with a task role. There are no long-lived AWS keys.

- **Planner role** in each env account (read-only, plus CloudWatch and Cost Explorer reads). The agent process
  assumes it for observing, planning, monitoring and cost.
- **Deployer role** in each env account (writes, limited to `iae:managed` resources, with a permissions
  boundary, no IAM wildcard).
- **Phase 1:** the agent process can assume the deployer role, and only the engine's apply path does so, after
  the policy and approval checks. The LLM has no tool that reaches it.
- **Phase 2 (before prod):** a separate **executor** service (same engine code, no LLM, its own task role) is the
  only principal that can assume deployer roles. The agent sends it (env, target commit, plan hash, approval
  link). The executor recomputes the plan, re-evaluates the policy and re-verifies the approval against Slack with
  its own token before applying. A compromised agent process can then propose but not mutate.

Other credentials, in Secrets Manager in the tooling account:

- **GitHub App** "iae": private key and webhook secret. Installation tokens for cloning, branches, PRs, statuses,
  deployments, and reading reviews for evidence. Branch protection lets the app push only `iae/` branches.
- **Slack**: bot token and Socket Mode app token.
- **Bedrock**: zio-bedrock authenticates with a Bedrock API key (bearer token), not SigV4. The agent mints
  short-term Bedrock API keys from its task role and refreshes them; a long-term key in Secrets Manager is the
  fallback (open question Q13).

### 5.12 Running and deploying the agent

- `iae bootstrap` (run once by a human with admin credentials) creates the tooling account pieces: the agent
  service, SQS inbox, `iae-webhook` Lambda, EventBridge rules (including cross-account forwarding from env
  accounts), CodeBuild `iae-build`, roles, secrets placeholders, plus the per-env roles, evidence buckets and Audit
  Manager setup (§6).
- The agent is an `InfraApp` itself: its inbox queue, roles and service are declared with zio-iae. After bootstrap
  the agent deploys new versions of itself through its own pipeline, and a built-in policy rule requires
  `Approve(1, owners)` for any change to the agent's own deployment.
- One running task (desired count 1). ECS deploys it stop-before-start, so there's a short gap rather than two
  agents. Events queue in SQS meanwhile, and the commit guard covers any overlap.
- Locally, `iae-agent` runs against Floci (AWS), a test Slack workspace, a webhook forwarded from GitHub (or
  replayed fixtures), and a real or scripted model.

---

## 6. Audit Manager integration

`iae bootstrap` creates, per env account:

- A custom framework **"IAE Change Management"** with custom controls (manual evidence sources):
  - `IAE-CM-1` change originates from a reviewed commit on `main`
  - `IAE-CM-2` automated tests passed for that commit
  - `IAE-CM-3` plan approved as required by policy: matched rules, policy commit, Slack request permalink,
    approvers and reaction times (or auto-approved by policy commit X)
  - `IAE-CM-4` deployment verified (health and canary results)
  - `IAE-CM-5` exceptions: break-glass, rollbacks, failed applies, drift reverts
- An assessment over that framework scoped to the env account, plus optionally a standard framework (such as SOC 2)
  so CloudTrail/Config automated evidence about the same resources sits next to ours.
- An evidence bucket `iae-evidence-<env>` with Object Lock (compliance mode) and versioning.

For each reconcile of an env, the engine writes one JSON evidence document per control to the evidence bucket
(commit, PR + reviewers, test summary, plan hash + changes, approval refs, per-change apply results, CloudTrail event IDs,
canary results, and links to the LLM transcripts of any agent tasks involved, §5.4) and imports it with `BatchImportEvidenceToAssessmentControl` (`s3ResourcePath`). Evidence is
batched per run because manual evidence is limited to 100 uploads per control per day.

Audit Manager has no Floci emulation. `iae-testkit` provides `AuditManagerFake` (records imports, enforces the
daily limit), and contract tests run against a real account when gated on.

---

## 7. Alert triage

- An alarm reaches the monitor (§5.10), which opens a `Triage` task and a Slack thread for the alert.
- The agent investigates with read-only tools: recent deployments and their canary results, CloudWatch logs and
  metrics scoped to the app, recent commits and PRs, the current plan (is there drift?).
- Possible outcomes:
  1. **Recent deploy correlates:** the agent calls `rollback`. The engine runs it if policy allows it for
     `origin = Alert` (it returns to a state that was already approved); otherwise it posts an approval request.
  2. **Code fix:** the agent makes the change in a workspace, builds it in CodeBuild with a regression test, and
     opens a PR linking the alert. It goes through review and the normal pipeline, with canary.
  3. **Infra tuning** (queue visibility, concurrency, capacity): a PR changing the `Infra` declaration.
  4. **Unknown:** the agent posts its triage notes as an escalation and asks for guidance.
- The agent keeps the alert thread open until the alarm clears, then posts a summary. It never mutates prod
  outside the pipeline; rollback is the only fast path, and it's policy-gated.

---

## 8. Repository layout

```
zio-iae/
  build.sbt                    # sbt 2 multi-project (bootstrapped via start.jamesward.com)
  AGENTS.md                    # project facts; follows zen-of-projects
  SPEC.md                      # this document
  modules/
    core/  aws-client/  aws/  testkit/       # library (Scala LTS)
    engine/                                  # plan, policy, apply, deploy, evidence (library used by agent + CLI)
    cli/                                     # `iae` CLI (and optional MCP server mode)
    agent/                                   # always-on agent: inbox, reconcilers, jobs, Slack, zio-bedrock tasks
    webhook/                                 # iae-webhook Lambda: verify GitHub signature, enqueue to SQS
  agent/
    prompts/                   # system prompts per LLM task (ExplainPlan.md, DiagnoseFailure.md, ...)
  examples/
    photo-app/                 # InfraApp using Bucket + Queue; end-to-end on Floci
  .github/workflows/           # CI for this repo (build + Floci tests)
```

A repo managed by zio-iae has:

```
.iae/config.json               # app entry points, envs + accounts/regions, promotion order,
                               #   Slack channel, responder group, GitHub-to-Slack identity mapping
.iae/Policy.scala              # the user's approval policy (§5.5)
CODEOWNERS                     # policy owners for .iae/Policy.scala
```

and the "iae" GitHub App installed.

---

## 9. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | Project bootstrap via start.jamesward.com (`AGENTS.md`, sbt 2, CI, sbta) | CI green on an empty module set |
| M1 | `iae-core` model; `iae-aws-client` SigV4 + S3; `Bucket` capability + provider; `FlociLayer`; manifest | Example app reads and writes a bucket on Floci; provider convergence tests pass |
| M2 | Engine: plan, policy DSL + evaluation + `iae policy explain`, idempotent apply, commit guard, tag discovery; `Queue`, `Table`, `Secret`, `Topic`; IAM generation; `iae` CLI | `assertConverges` end-to-end on Floci for the example app, including crash/re-run |
| M3 | Agent process: webhook Lambda + SQS inbox, per-env reconcilers, CodeBuild builds, Slack Socket Mode, approval requests + verification, notices, rehydrate on restart; staging account | A push to `main` converges staging; a gated change waits for ✅; killing the agent mid-run and restarting it converges with no lost approval |
| M4 | zio-bedrock tasks: `ExplainPlan`, `DiagnoseFailure`, `Converse`; agent-authored PRs; `ScriptedModel` tests; Audit Manager evidence; executor split; prod account | A failing build produces a diagnosis and a fix PR; a prod change with approval produces evidence for IAE-CM-1..4 |
| M5 | `Service` + ECR + ALB canary + auto-rollback | A bad build in staging is rolled back automatically by an alarm |
| M6 | Monitor + drift jobs; `Triage` task | A synthetic alarm results in a rollback or a fix PR; a hand change is reported and reverted under policy |
| M7 | Cleanup + cost optimizer jobs | Orphans and old images are proposed for cleanup; a weekly cost report with at least one right-sizing PR |

---

## 10. Open questions

| # | Question | Current leaning |
|---|---|---|
| Q1 | Package and artifact naming: `zio.iae` / `dev.zio`-style vs `com.jamesward` | `zio.iae` package, `com.jamesward` org, unless it moves under the ZIO org |
| Q2 | Resource identity via singleton-type `Tag`s: verify izumi-reflect stability; fallback to marker traits | Spike in M1 |
| Q3 | Hand-written `iae-aws-client` vs zio-aws vs Java SDK v2 | Hand-written for the ops we need (ZIO-native streams, Scala 3, small footprint); revisit if the surface grows |
| Q4 | Manifest extraction: running `main --iae-manifest` via sbt per commit is slow | Do it in CodeBuild alongside the build; keep the result in memory for the commit; later an sbt task that only links the `infra` value |
| Q5 | When to split out the executor (§5.11) | Before managing prod (M4) |
| Q6 | Does the agent need durable memory beyond what it can rehydrate (learned facts about a system, runbooks)? | If so, as markdown in the repo (`.iae/agent/notes/`) written through PRs, so it's reviewed and versioned rather than a hidden store |
| Q7 | Multi-app repos and shared infra (VPC, ALB, DNS zone) owned by a platform `InfraModule` | Platform module with its own Slack channel and policy; apps reference it by id |
| Q8 | Multi-region / multi-account per env | Out of scope until M5 |
| Q9 | High availability: one agent task vs. leader election | One task; SQS buffers during restarts and the commit guard covers overlap. Revisit if restart gaps hurt |
| Q10 | Commit guard read-then-write race during agent redeploys | Accept; stop-before-start makes the overlap rare and the next reconcile converges |
| Q11 | Slack retention and audit: approvals and questions must stay readable at least until they expire; long-term proof is the S3 evidence copy | Require channel retention ≥ 30 days; checked at bootstrap |
| Q12 | Reactions (v1) vs buttons with a separate approvals app (v2) | Reactions first: unforgeable attribution and no extra endpoint |
| Q13 | Bedrock auth for zio-bedrock: short-term API keys minted from the task role vs a long-term key | Short-term keys if minting from role credentials works reliably; otherwise a rotated key in Secrets Manager |
| Q14 | Mapping GitHub authors to Slack users for the "author can't approve" rule | Explicit mapping in `.iae/config.json`; fall back to email match through Slack's `users.lookupByEmail` |
| Q15 | Floci coverage for Cost Explorer, Compute Optimizer, Config and Bedrock | Fakes in `iae-testkit` where Floci lacks them; `ScriptedModel` for Bedrock |

---

## 11. Next step

Bootstrap the repository with the instructions at <https://start.jamesward.com> (the `zen-of-projects` Skill):
sbt 2, `AGENTS.md`, CI, sbta. Then lay out the modules in §8 and start M1.
