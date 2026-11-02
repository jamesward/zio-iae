# zio-iae: Infrastructure as Effects

Status: **Draft v0.2** (2026-10-31)

zio-iae makes cloud infrastructure part of a ZIO program's type. An app that needs an S3 bucket asks for one in its
environment (`ZIO[Bucket[Uploads], ...]`) the same way it asks for any other service. The infrastructure is
declared once and used for three things:

1. **At runtime** it provides the ZIO layers the program uses (`Bucket`, `Queue`, ...).
2. **At deploy time** it is the desired-state description that an engine reconciles against AWS.
3. **For security** it generates the app's least-privilege IAM policy from the capabilities the program uses.

An always-on, stateful agent process runs the pipeline. It receives git pushes through its own GitHub webhook
(or, with the repo in AWS CodeCommit, through EventBridge with no webhook at all; §5.2.1),
plans and applies infrastructure changes (including changes it authors itself), deploys the app with canaries, and
records evidence in AWS Audit Manager. The same process runs monitors, drift detection, security audits, resource cleanup and cost
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

## Why an agent

Traditional IaC (Terraform, CDK, Pulumi, CloudFormation) is a compiler for infrastructure: a human writes the
desired state, CI runs `plan` and `apply` when the code changes, and the job ends. Everything after the apply
(watching the system, noticing waste, keeping it secure, fixing what broke, cleaning up) is left to people, and
in most teams that work is done late or not at all. An agent that owns the infrastructure keeps working after
the deploy.

| | Traditional IaC | Agent-managed infra (zio-iae) |
|---|---|---|
| **When it runs** | When someone pushes; idle otherwise | Always: pushes, alarms, drift, cost signals, security findings, timers |
| **Cost optimisation** | The declared sizes stay until someone reads a bill and edits the code | Watches Cost Explorer, utilisation and Compute Optimizer, and opens PRs that right-size services, change capacity modes or add lifecycle rules, with the expected savings |
| **Security audits** | Periodic manual reviews or a separate scanner whose findings land in a backlog | Continuously reads IAM Access Analyzer, Security Hub, GuardDuty and Inspector findings for the resources it manages, and opens fixes as PRs: drop unused permissions, patch a vulnerable image, close an exposure |
| **Least privilege** | Hand-written IAM, usually broader than needed and drifting from the code | Generated from the capabilities the code uses, then narrowed further from observed unused access |
| **Delivery ("auto-DevOps")** | A pipeline someone has to build, debug and maintain; a failed build waits for a human to read the log | Builds, tests, plans, deploys with canaries and rolls back on its own; when a build or deploy fails it diagnoses the failure and proposes the fix |
| **Incidents** | A human is paged, finds the cause, and fixes it by hand or by a rushed change | Triages the alarm against recent deploys, logs and metrics; rolls back if policy allows, or opens a fix PR, and keeps the alert thread updated |
| **Drift** | Found at the next `plan`, often weeks later, and usually overwritten without anyone asking why | Detected when it happens, reported with who changed what, and reverted or turned into a code change, as policy says |
| **Cleanup** | Orphaned and idle resources pile up because nobody is sure what's safe to delete | Finds orphans, idle resources and stale artifacts, explains them, and deletes them only with the approvals policy requires |
| **Change review** | Reviewers read a raw `plan` diff | The agent explains each change in plain language, with blast radius and rollback, and quotes the policy clauses that apply |
| **Approval rules** | All-or-nothing: a protected CI environment, or nothing | Per change, from a plain-English policy the team writes: what can go ahead, who must approve, what's never allowed |
| **Compliance evidence** | Assembled by hand before an audit | Recorded on every change (commit, tests, plan, verdict, approvers, results) and filed in AWS Audit Manager |
| **State** | A state file to store, lock and repair | None: the cloud's own APIs are the state |
| **People's time** | Spent operating: running pipelines, reading dashboards, chasing tickets | Spent deciding: approving, rejecting and guiding in Slack when the agent asks |

The agent doesn't get unchecked power for this. It changes infra only through the engine. The user's policy
decides what needs a human. Approvals come from people and are verified, so the agent can't produce them.
Everything it does is explained in Slack and recorded as evidence.

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
- **The agent can change infra; the user's policy decides when a human must approve.** For now the policy is plain
  English in the user's repo (`.iae/POLICY.md`), applied by an LLM to the structured facts of each change (env,
  resource, change type, risk, who authored it), inside a fixed floor of built-in rules that fails safe toward
  asking a human. The agent never holds approval authority, and every mutation goes through the engine, which
  enforces the verdict.
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
| **Policy** | The user's approval rules, written in plain English in `.iae/POLICY.md`. Applied to each change (env, resource, change type, risk, origin) to give `Auto`, `Approve(n, groups)` or `Deny`. |
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
| `iae-testkit` | `iae-core`, `iae-aws-client`, `testcontainers-floci-aws` | `FlociLayer` (a Floci container and an `AwsConfig` for it), `InMemoryCloud` (in-memory capabilities for fast app tests), fakes for services Floci doesn't emulate (Audit Manager), in-memory queues and secrets with SQS-like delivery. `iae-aws` uses it in its own tests (convergence helpers live in the engine's tests, since the engine depends on `iae-aws`). |

Library modules use the Scala LTS (per the `zen-of-projects` compatibility rules). Package: `zio.iae` (see open
question Q1).

### 3.2 Resource identity

ZIO environments are keyed by type, so two buckets need two types. Every resource is identified by a singleton
object that carries its logical id and spec:

```scala
object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
object Thumbs  extends Bucket.Id(LogicalId("thumbs"),  BucketSpec(lifecycle = List(Expire(after = 30.days))))
object Jobs    extends MessageQueue.Id[Job](LogicalId("jobs"), QueueSpec(visibilityTimeout = 30.seconds, deadLetter = Some(DeadLetter(maxReceives = 5))))
```

`LogicalId("…")`, `AppName("…")` and `EnvName("…")` are opaque types whose literals are checked at compile time
(lowercase letters, digits and single hyphens, starting with a letter, with a length limit), since they end up in
physical resource names. Values known only at runtime go through `LogicalId.from`, which returns an `Either`.

The capability types are parameterised by the id's singleton type: `Bucket[Uploads.type]`. A type alias keeps
signatures readable (`type Uploads = Uploads.type`). M1 confirmed that `izumi-reflect` `Tag`s for singleton object
types work across compilation units: the example app declares `Uploads` in one module and its tests provide
`Bucket[Uploads.type]` from another.

Logical ids are stable. Renaming an id is a replace (delete + create) and the plan shows it as such, unless the
id declares `formerly = "old-id"`, which turns it into a move.

### 3.3 Capability traits

Each resource kind has a full trait and narrower capability traits. The engine derives IAM actions from the
narrowest capability the app requests.

```scala
trait BucketReader[I]:
  def get(key: ObjectKey): ZStream[Any, BucketError, Byte]
  def head(key: ObjectKey): IO[BucketError, Option[ObjectMeta]]
  def list(prefix: Option[ObjectKey] = None): ZStream[Any, BucketError, ObjectMeta]

trait BucketWriter[I]:
  def put(key: ObjectKey, body: ZStream[Any, Throwable, Byte], contentType: Option[String] = None): IO[BucketError, Unit]
  def delete(key: ObjectKey): IO[BucketError, Unit]

trait Bucket[I] extends BucketReader[I], BucketWriter[I]

enum BucketError:
  case NotFound(key: ObjectKey)
  case AccessDenied(detail: String)
  case BodyFailed(detail: String)    // the stream passed to `put` failed
  case Unavailable(detail: String)   // anything else, after the client's retries
```

Errors are typed and specific to the resource. Throttling, server errors and network failures are retried with a
`Schedule` inside the client and only surface (as `Unavailable`) when retries run out. Pre-signed URLs
(`presignGet`, `presignPut`) and multipart uploads for large bodies come later; `put` currently buffers the body.

MVP resource kinds and capabilities:

| Kind | AWS | Capabilities | Stateful (retain by default) |
|---|---|---|---|
| `Bucket` | S3 | `BucketReader`, `BucketWriter`, `Bucket` | yes |
| `MessageQueue` | SQS (+DLQ) | `QueueSender`, `QueueReceiver`, `MessageQueue` | yes (messages) |
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
MessageQueue.sender(Jobs): Infra[QueueSender[Jobs]]
Topic.publisher(Events).subscribe(Jobs)  // wiring is part of the declaration
```

Resources that are only wired together (such as a DLQ, or a bucket notification to a queue) are declared without
granting the app a capability on them.

`Cloud` is the runtime binding: given a logical id and spec, it returns the capability. On AWS it's `AwsCloud`
(app, env, provision mode, naming, S3 client), configured by `AwsCloud.fromEnvironment` from `IAE_ENV`,
`IAE_PROVISION`, `IAE_NAME_SUFFIX` and the standard AWS variables (including `AWS_ENDPOINT_URL` for Floci). Tests
use `AwsCloud.layer` against Floci or `InMemoryCloud` from `iae-testkit`.

At runtime, `layer` **binds, it doesn't create** (`ProvisionMode.BindOnly`, the default). It resolves the physical
name and checks, once at startup, that the resource exists with the expected `iae:spec-hash` tag. If not, it fails
fast with `InfraError.NotProvisioned` (the engine hasn't converged this env yet) or `InfraError.SpecMismatch` (the
resource was provisioned from another spec or changed by hand). `ProvisionMode.AutoProvision`
(`IAE_PROVISION=auto`) creates or converges resources directly through the providers, so a local run against Floci
needs no engine.

### 3.5 `InfraApp`

```scala
object PhotoApp extends AwsInfraApp(AppName("photo-app"), Bucket.readWrite(Uploads) ++ Bucket.readWrite(Thumbs) ++ MessageQueue.sender(Jobs)):
  def program: ZIO[Bucket[Uploads.type] & Bucket[Thumbs.type] & QueueSender[Jobs.type], Any, Any] =
    for
      _ <- ZIO.serviceWithZIO[Bucket[Uploads.type]](_.put(key, body))
      _ <- ZIO.serviceWithZIO[QueueSender[Jobs.type]](_.send(Job.Resize(key)))
    yield ()
```

Queues are named `MessageQueue` (not `Queue`) so they don't clash with `zio.Queue` under `import zio.*`. A queue is
declared with its message type (`MessageQueue.Id[Job]`), messages travel as JSON from the type's `Schema`, and
`send`/`receive` only accept that type: they take evidence `I <:< MessageQueue.Id[M]`, so `M` is inferred from the
queue and sending anything else is a compile error. Tables work the same way: `Table.Id[K, V]` stores JSON keys and
values in a DynamoDB table, and `get`/`put`/`delete` infer `K` and `V` from the declaration. A topic is declared with
its message type and its subscribers, `Topic.Id[M](id, List(Topic.to(Thumbnailer)))`, where `Topic.to` only
accepts a `MessageQueue.Id[M]`, so a queue of another message type doesn't compile; subscribed queues get every
message as-is (raw delivery) and must be declared by the app (`MessageQueue.wired` declares one without granting
access). The engine creates resources in kind order (buckets, queues, secrets, tables, topics, then the role), so
queues exist before topics subscribe them. `Secret.reader(DatabasePassword)` gives a `SecretReader` whose
value is set out of band and redacted in `toString`; the engine creates and tags the secret but never handles its
value.

`InfraApp[R]` takes the infra as a constructor argument, so `R` is inferred from it and `program: ZIO[R, Any, Any]`
can't require a resource that isn't declared (a compile error). `AwsInfraApp` is the AWS-bound subclass. Its
`main`:

- `--iae-manifest` writes the manifest JSON to stdout and exits without contacting AWS. The engine uses this to
  get the desired state at a commit.
- Otherwise it builds `infra.layer` from `AwsCloud.fromEnvironment` and runs `program`. An `InfraError` (such as
  `NotProvisioned`) is logged as one plain message and the process exits with status 1.

Apps that aren't a single `main` (a library module of shared infra, or several services in one repo) extend
`InfraModule` (just `val infra`), and the repo's `.iae/config.json` lists the entry points.

### 3.6 Naming, tagging, ownership

- Physical name: `<app>-<env>-<id>`, plus a suffix where AWS needs global uniqueness (S3), set by
  `IAE_NAME_SUFFIX` for now (for example a short hash of the account id).
- Every managed resource is tagged `iae:managed`, `iae:app`, `iae:env`, `iae:id`, `iae:kind`, `iae:spec-hash`,
  and (from M2) `iae:commit`. Tags the user adds by hand are kept when the engine re-tags.
- **Tags are the ownership record.** There is no state file, so nothing can drift from reality. The engine
  discovers what it owns through the Resource Groups Tagging API (S3, SQS, Secrets Manager, DynamoDB, SNS) and, because that API
  doesn't cover IAM on AWS, by listing roles under the `/iae/` path. `iae:commit` is
  also the concurrency guard (§5.3).
- IAM policies for the deploy role are conditioned on `aws:ResourceTag/iae:managed = true`, so the engine
  can't touch resources it doesn't own.

### 3.7 IAM generation

From `Infra.grants` the manifest derives one `Role` resource (logical id `runtime`) per app, and the engine
provisions it per env as `iae-<app>-<env>` under the `/iae/` path, with one inline policy statement per (capability,
resource). For example `BucketReader[Uploads]` gives `s3:GetObject` and `s3:ListBucket` on that bucket only, and
`QueueSender[Jobs]` gives `sqs:SendMessage` on that queue only. Queue and secret ARNs need the account, which the
engine reads once from STS (`AwsContext`). The role is itself a managed resource, so a permission change is a plan
change: adding permissions is `High` and flagged `IamWidening`, removing them is `Medium` (see §5.5).

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
| Unit | `diff`, `risk`, verdict combination and the built-in floor, plan ordering, manifest codec round trip, IAM generation | pure, property-based (zio-test `Gen`) |
| Client | Each AWS op | Floci |
| Provider | create, then observe, then diff is empty; update; replace; delete; `apply` idempotency | Floci |
| Capability | `Bucket[I]` etc. used by a sample program | `InMemoryCloud`, then Floci with `ProvisionMode.AutoProvision` |
| Engine end-to-end | manifest, plan, apply, re-plan gives an empty plan (`assertConverges`); crash mid-apply then re-run converges; an older commit can't overwrite a newer one (commit guard) | Floci |
| Agent | Inbox routing and coalescing, reconciler flow, approval verification, restart rehydration, job candidate detection | Floci + `FakeSlack` + `FakeGitHub` |
| Scenarios | The `Scenario`s from the local harness (§4.1), run headlessly end to end | Floci + fakes + `ScriptedModel` |
| LLM tasks | Tool dispatch, budgets, safety rules (no mutation without policy), prompt-injection fixtures; `EvaluatePolicy` fail-safe paths (bad output, unknown group, unconfident, made-up clause) | `ScriptedModel` (zio-bedrock custom origin) |
| Policy evals | `EvaluatePolicy` against the example policy and `.iae/policy-examples` | Real model (gated, paid) |
| Audit Manager | evidence import | in-memory fake; real AWS gated (`IAE_AWS_CONTRACT_TESTS=1`) |
| Real AWS smoke | One sandbox account, nightly | AWS (gated, paid) |

`iae-testkit` provides:

```scala
object FlociLayer:
  val image: String                                        // pinned floci/floci tag
  val container: ZLayer[Any, Throwable, FlociContainer]   // ZLayer.scoped around io.floci:testcontainers-floci-aws
  val awsConfig: ZLayer[FlociContainer, Throwable, AwsConfig]
  val live:      ZLayer[Any, Throwable, AwsConfig]         // container >>> awsConfig

object InMemoryCloud:
  def layer(app: AppName, env: EnvName): ULayer[Cloud]     // capabilities backed by a Ref, for fast app tests
```

An app's tests can run the same suite at two layers of the test onion: against `InMemoryCloud`, then against Floci
through `AwsCloud.layer(app, env, ProvisionMode.AutoProvision, naming)`. The example app does exactly that.

One container per suite (`provideShared`). Each test gets a fresh `env` name so tests don't collide. Floci's
emulated services include S3, SQS, SNS, DynamoDB, IAM, STS, KMS, Secrets Manager, ECS, ECR, ELBv2, CloudWatch,
CloudTrail and Config. **Audit Manager isn't emulated**, so it needs the fake. Floci emulates; it isn't AWS, so
provider behaviour that matters (IAM evaluation, S3 consistency edge cases) also gets real-AWS smoke coverage.

### 4.1 Local development harness (`AppTest`)

`./sbta agent/Test/run` starts the whole system on a laptop: the real agent, wired to emulated AWS, Slack and
GitHub, with a web UI that stands in for the Slack channel and lets you simulate pushes, deployments and
incidents. `./sbta ~agent/Test/runReload` restarts it on code changes. **Everything except the LLM is emulated.**
The LLM is real Bedrock through zio-bedrock (`AWS_BEARER_TOKEN_BEDROCK`, `BEDROCK_MODEL_ID`), or acprock; without
them `AppTest` fails at startup with a message saying which variable is missing.

All of it lives in `modules/agent/src/test/scala` (and test-scoped dependencies), so none of it reaches the
production classpath. `AppTest` composes the production agent with test layers, and production code has no dev
branches. Wherever possible the **real clients** are pointed at emulated endpoints (Slack base URL, GitHub API URL,
`AWS_ENDPOINT_URL`), so the harness exercises the same HTTP code as production.

| Real service | Emulated by | Notes |
|---|---|---|
| AWS accounts (tooling, staging, prod) | One Floci container per account | S3, SQS, SNS, DynamoDB, IAM, STS, Secrets Manager, ECR, ECS, ELBv2, CloudWatch, CloudTrail, Config, EventBridge, CodeBuild, Lambda |
| AWS services Floci lacks | In-process fakes over HTTP from `iae-testkit`: `AuditManagerFake`, `CostExplorerFake`, `ComputeOptimizerFake`, `CostAnomalyFake` | The real `iae-aws-client` calls them through per-service endpoint overrides |
| Slack (Web API + Socket Mode) | `FakeSlack`: the subset the agent uses (`chat.postMessage`, `chat.update`, `conversations.history`/`replies`, `reactions.get`, `usergroups.users.list`, `users.info`, `users.lookupByEmail`, `apps.connections.open` + the Socket Mode websocket) | State in memory; the web UI is its client |
| GitHub (repo, App API, webhooks) | A local bare git repo as the remote, plus `FakeGitHub`: the App API subset (installation tokens, PRs, reviews, merges, statuses, deployments) and a webhook sender that signs `push` and `pull_request` events with the configured secret | Pushing to the local repo, from the UI or a real `git push`, sends the webhook |
| CodeCommit (alternative git host, §5.2.1) | The same local bare repo, plus pushes published as `CodeCommit Repository State Change` events to Floci's EventBridge, whose rule targets the inbox; `CodeCommitFake` for pull requests | The trigger path is the production rule, not a shortcut into the inbox |
| `iae-webhook` Lambda | The same handler code, run in-process behind a local HTTP endpoint | Enqueues to the tooling account's SQS in Floci, as in production |
| Bedrock | **Not emulated** | Real model; token usage shows in the UI |

**Seed.** On start the harness creates the accounts' bootstrap resources (`iae bootstrap` against Floci), seeds the
local repo from `examples/photo-app` with a `.iae/config.json`, a sample `POLICY.md` and policy examples, and
creates simulated Slack users: `alice` (in `infra-approvers`), `bob` (in `platform-owners`), `carol` (in no
group) and `dev` (the author of seeded commits).

**Web UI** (served by zio-http on `localhost`, test scope only):

- **Slack.** The `#iae-<app>` channel with threads, rendered from `FakeSlack`'s state and updated live. A user
  switcher picks who you are; you can reply in threads, mention the bot, and add ✅/❌ reactions. Because it goes
  through `FakeSlack`, the agent sees exactly what it would see from real Slack, and approvals are verified the
  same way (group membership, author exclusion, expiry).
- **GitHub.** Commits on `main`, open PRs (including the agent's), review and merge buttons, and a small editor to
  change a file and push, which sends the webhook.
- **Environments.** For staging and prod: managed resources and their tags, the deployed sha, canary weights and
  bake timers, alarms, and recent evidence.
- **Agent.** Inbox events, reconciler status per env, running tasks with their transcripts and token usage, open
  waits (approvals, questions) with timers.
- **Simulate.** The controls below.
- **Clock.** A speed factor for the agent's `Clock` (default 60×), so reminders, approval expiries, bake windows
  and daily jobs happen in minutes, plus "advance by…" for jumps. The LLM, Floci and Docker run in real time.

**Simulating deployment.** Builds run in Floci's CodeBuild, which uses real Docker, and push the example app's
image to Floci's ECR. The engine deploys it to Floci's ECS, where it runs as a real container, behind ELBv2
target groups with weighted forwarding. A traffic generator in the harness sends steady requests to the service
and publishes request, error and latency metrics to Floci's CloudWatch, so the canary's alarms are evaluated on
real numbers. The Environments panel shows the canary stepping through its weights.

**Simulating issues.** Each control is a `Scenario` step (below), available as a button:

| Issue | How it's simulated |
|---|---|
| Bad deploy | Push a commit (from a patch shipped with the example) that makes the app return 500s for some requests; the canary's alarm fires and the engine rolls back |
| Faulty canary | Fault controls per env make the version in canary answer 5xx, answer slowly or report failing uploads (the app's own metric); its 5xx, latency or app alarm rolls it back (`slow-deploy`, `failing-metric`) |
| Production incident | Turn up the example app's fault rate or latency through its test-only fault endpoint, so real metrics trip real alarms; or force an alarm with `SetAlarmState` |
| Build or test failure | Push a commit with a failing test or a compile error; `DiagnoseFailure` runs |
| Gated change | Push an infra change the sample policy gates (a deletion in prod, IAM widening); approve or reject it in the Slack panel as different users, including one who isn't allowed to |
| Drift | Change a managed resource directly through the Floci API as a different principal (for example a queue's visibility timeout) |
| Orphans and idle resources | Remove a resource from the manifest; create `iae:`-tagged resources no repo declares; set idle metrics |
| Cost anomaly | Post a Cost Anomaly event and edit the fake Cost Explorer series |
| Security finding | Grant a role an unused permission, make a bucket public through the Floci API, or post a GuardDuty or Inspector finding |
| Nobody answers | Let a question or approval sit while the clock runs fast; watch the reminder, the escalation and the expiry |
| Agent restart | Stop and restart the agent (same harness, new agent layer) mid-reconcile, mid-canary or while a question is open, and check it rehydrates |
| Dropped event | Push without sending the webhook; the periodic reconcile picks it up |

Where Floci doesn't emit an event the agent relies on (for example an EventBridge event for a CloudTrail change
or an alarm state change), the harness publishes the same event with `PutEvents`, so the agent's input is the
same as in production.

**Scenarios are code.** A `Scenario` is a list of steps (push a patch, wait for a Slack message matching…, react
as `alice`, inject faults, advance the clock, assert on an env's state). The UI runs them step by step. The
end-to-end agent tests run the same scenarios headlessly with `ScriptedModel` in place of Bedrock, so every issue
the harness can simulate is also a repeatable test.


---

## 5. The agent

### 5.1 Architecture

The agent is an **always-on, stateful Scala/ZIO process** (`iae-agent`) running in a dedicated tooling AWS
account. It contains the deterministic engine as a library and uses
[zio-bedrock](https://github.com/jamesward/zio-bedrock) for the LLM parts. There is no Claude Code routine: we
receive git pushes through our own GitHub webhook, and the long-running jobs (reconcilers, monitors, cleanup, cost
optimizer) are fibers in the same process.

```
 GitHub, Slack ──signed webhooks──▶ iae-webhook (verifies signatures) ──▶ SQS inbox ─┐
 CodeCommit (alternative to GitHub) ──EventBridge rule──▶ SQS inbox ──────────────────┤
 CloudWatch alarms / Config / CloudTrail / Cost Anomaly ──EventBridge──▶ SQS ─────────┤
 Slack ◀──Web API (posts, history, reactions, groups)─────────────────────────────┐   │
                                                                                  ▼   ▼
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
| Build manifest, observe, plan, classify risk; build change facts; combine verdicts and enforce the built-in floor | Apply the user's natural-language policy to change facts (`EvaluatePolicy`) |
| | Diagnose failed builds, tests, applies and canaries |
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
| Push to `main` (CodeCommit) | `CodeCommit Repository State Change` event through an EventBridge rule straight to the SQS inbox (§5.2.1) | Same |
| Pull requests | Same webhook (`pull_request`), or `CodeCommit Pull Request State Change` events through EventBridge | Plan preview + policy status check `iae/policy` on the PR; agent-authored PR flow (§5.5) |
| Slack | Slack's Events API to the same webhook (`/slack`, signature and a 5-minute replay window checked), reduced to `SlackReaction`/`SlackReply` inbox events | Approvals (§5.6), conversations (§5.7) |
| Alarms | CloudWatch alarm state changes through EventBridge to the inbox | Monitor job (§5.10) |
| Drift | AWS Config / CloudTrail events on `iae:managed` resources by a principal other than the deployer role, through EventBridge | Drift job (§5.10) |
| Cost | Cost Anomaly Detection through EventBridge; scheduled Cost Explorer reads | Cost job (§5.10) |
| Security | Security Hub, GuardDuty and Inspector findings through EventBridge; scheduled Access Analyzer reads | Security auditor (§5.10) |
| Build results | CodeBuild state changes through EventBridge | Reconciler waiting on that build |
| Timers | `Schedule`s in the process: periodic full reconcile (default 15 min), cleanup (daily), cost review (daily, weekly report), reminders and expiries | The owning job |

The agent has **no inbound network endpoint** of its own. GitHub and Slack both reach it through the webhook and
SQS, so there is one input path and it is buffered (up to 14 days) while the agent restarts or redeploys; the
periodic reconcile covers anything that's lost anyway. (Socket Mode was the first plan; one signed webhook for both
turned out simpler.) Inbox events carry only identifiers: the agent re-reads git, the cloud and Slack, so a forged
or replayed event can at most cause an extra reconcile. The webhook is an HTTP server (`WebhookServer`), run on
Lambda behind the Lambda Web Adapter or anywhere else.

### 5.2.1 Git host: GitHub or CodeCommit

The repo can live on GitHub (built first) or in [AWS CodeCommit](https://aws.amazon.com/codecommit/), which
returned to general availability for new customers in November 2025. Everything after "a commit landed on `main`"
is the same: the agent fetches the branch, builds in CodeBuild, plans, asks and applies. Only four things depend on
the host: how a push reaches the inbox, how the agent and executor authenticate to git, how pull requests work, and
who the pusher was.

| | GitHub | CodeCommit |
|---|---|---|
| Push trigger | App webhook to `iae-webhook`, signature checked | `CodeCommit Repository State Change` event (`referenceCreated`/`referenceUpdated` on `refs/heads/main`) matched by an EventBridge rule whose target is the SQS inbox. An input transformer turns it into the same `Pushed` inbox event. No public endpoint and no shared secret; the queue policy admits only that rule |
| Git auth | Installation tokens from the GitHub App | IAM: the agent's and the executor's task roles, with a SigV4 git credential helper (as `git-remote-codecommit` does). Nothing to store or rotate |
| Who may push where | GitHub rulesets | IAM. The agent's role has `codecommit:GitPull` and `codecommit:GitPush` with a `codecommit:References` condition limited to `refs/heads/iae/*`, so it can't push to `main` whatever the model asks. The executor's role only pulls |
| Pull requests | GitHub PRs; plan preview as a status check and comment | `CreatePullRequest`; plan preview as a PR comment (`PostCommentForPullRequest`); `CodeCommit Pull Request State Change` events through EventBridge. Approval rule templates take the place of required reviews; the agent merges its own PR (§5.5) only where they allow |
| Pusher identity | The commit author's email (GitHub doesn't vouch for it) | The commit author's email, plus the IAM principal that pushed (`callerUserArn` in the event), recorded in the `IAE-CM-1` evidence |

What CodeCommit gives up:
- a familiar review UI and the GitHub ecosystem (Actions, apps, Copilot review);
- Git LFS, which is on AWS's roadmap;
- regions it isn't offered in.

What it gains:
- everything stays inside the AWS account and IAM, with CloudTrail recording every push;
- one less secret (the GitHub App key) and one less inbound path;
- `iae-webhook` then only serves Slack, or goes away with Slack's Socket Mode.

Configuration: the agent and executor tell the host from `IAE_REPO_URL`. A `codecommit::<region>://<repo>` URL or
`https://git-codecommit.<region>.amazonaws.com/v1/repos/<repo>` means CodeCommit. The repository should be in the
tooling account, so the EventBridge rule and the inbox are in the same account. A repository elsewhere forwards its
events to the tooling account's event bus. `iae bootstrap` creates the rule, the queue policy and the roles'
CodeCommit permissions.

Testing: Floci doesn't emulate CodeCommit, but it does emulate EventBridge.
- The harness keeps its local bare repo as the remote. In CodeCommit mode its push panel publishes the
  CodeCommit-shaped event with `PutEvents` to Floci's EventBridge, where the real rule (pattern and input
  transformer) routes it to the inbox. That tests the trigger path end to end.
- The pull request API subset gets a `CodeCommitFake` in `iae-testkit`, like `AuditManagerFake`.

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
| `EvaluatePolicy` | Every plan, preview and gated action | none (structured output only) | A `ChangeVerdict` per change |
| `ExplainPlan` | Approval request posted | — | A thread reply explaining the plan |
| `DiagnoseFailure` | Failed build, test, apply or canary | workspace edit, `runBuild`, `openPr`, `ask` | A diagnosis, plus a fix PR or a question |
| `Converse` | Human message in a run thread or a mention of the bot | `ask`, `notify`, `openPr`, `requestReconcile` | A reply |
| `Triage` | Alarm (§7) | `rollback` (policy-gated), workspace edit, `runBuild`, `openPr`, `ask` | Rollback, fix PR, or escalation |
| `ReviewCleanup` | Cleanup job candidates | `proposeCleanup` | Proposals in Slack |
| `ReviewCost` | Cost job findings | workspace edit, `openPr` | A cost report, and PRs for changes worth making |
| `ReviewSecurity` | Security auditor findings | workspace edit, `runBuild`, `openPr`, `ask` | A security report, fix PRs, or an escalation |

**Built so far** (each needs a model configured; without one the agent runs without them):

- `ExplainPlan` replies in a new approval request's thread, in the background; a failure is only logged.
- `DiagnoseFailure` runs when a reconcile fails. The failure is posted once per env, commit and kind of failure,
  and the diagnosis goes in that post's thread. Its read-only tools (`listFiles`, `readFile`, `search`) read the
  failing commit from git's objects, so no checkout moves. With a GitHub App configured it also has `writeFile`,
  which edits a scratch clone of the failing commit, and `openPullRequest`, which pushes one `iae/fix-<sha>-<n>`
  branch and opens one pull request per failure. The push URL's token is redacted from any error text.
- `Converse` answers in the agent's threads and when it's mentioned. It answers only the latest message, only from
  members of the policy's groups, and handles conversations one at a time so no thread is answered twice. Its tools
  are read-only (`status`, `plan`, `policy`); `ask`, `notify`, `openPr` and `requestReconcile` are still to come.

- **System prompts** live in the repo (`modules/agent/src/main/resources/prompts/<task>.md`) so they're versioned
  and reviewed. Each task has
  a turn limit, a token budget and a timeout; daily budgets per task kind are enforced from zio-bedrock's usage
  metrics.
- **Read-only tools:** `observe`, `plan`, `policyExplain`, `status`, CloudWatch logs and metrics queries scoped to
  the app, Cost Explorer queries, git log and diff, file read and search in a workspace, Slack thread read.
- **Effectful tools** never mutate infra directly: editing files in a local workspace, `runBuild` (CodeBuild on a
  `iae/` branch), `openPr` / `updatePr` through the GitHub App, `ask` / `notify` in Slack, `requestReconcile`.
  The gated tools (`rollback`, `proposeCleanup`) go through the engine's policy evaluation and come back with
  `Done`, `NeedsApproval(requestLink)` or `Denied(clauses)`.
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

### 5.5 Approval policy (user-configured, natural language)

The user decides what needs a human, and for now writes it in plain English in `.iae/POLICY.md`. An LLM task
(`EvaluatePolicy`) reads the policy and the plan and returns a structured verdict. Deterministic code around it
supplies the facts, enforces a fixed floor of rules, and fails safe.

Example `.iae/POLICY.md`:

```markdown
# Approval policy

Approvers are the @infra-approvers Slack group. Owners are @platform-owners.

- Anything in dev can go ahead without asking.
- Never make a bucket public. Refuse that outright.
- In prod, deleting or replacing anything needs two owners.
- Any change that gives an app more IAM permissions needs one approver.
- If you (the agent) wrote the change and it's more than a low-risk change in prod, ask an approver.
- Changes to the `orders` table always need an owner, in every env.
- From Dec 20 to Jan 3 nothing goes to prod without an owner.
- Cleanup of old container images and task definitions is fine without asking.
- Otherwise: low and medium risk changes are fine; high risk needs an approver; critical needs two owners.
```

**Facts, not prose.** The engine builds a structured, typed description of each change and passes only that to
`EvaluatePolicy`: env; resource kind and id; change type (`Create`, `Update(fields)`, `Replace`, `Delete`, `Move`,
`Deploy`, `Rollback`, `Cleanup`); risk; derived flags (`iamWidening`, `publicAccess`, `encryptionChange`,
`dataLoss`); **origin** (`Human` for commits authored by people, `Agent` for commits the agent authored, `Alert`,
`Drift`, `Cleanup`, `Cost` and `Security` for changes the background jobs propose, §5.10); the projected monthly cost
change (§5.5.1, planned); author; current date and time. PR
descriptions, commit messages, Slack text and logs are not included, so they can't argue for a weaker verdict.

Risk classes, assigned by providers with these defaults (a provider can raise a specific change's risk):

| Risk | Examples |
|---|---|
| `Low` | Create; tag-only update; app deploy with no infra change |
| `Medium` | In-place config update (visibility timeout, lifecycle rules); IAM narrowing |
| `High` | Replace of a stateless resource; IAM widening; public-access, encryption or network change; new prod service |
| `Critical` | Delete or replace of a stateful resource; anything touching data retention |

**5.5.1 Cost projection.** Every plan says what it will cost, so people approve with the money in view and
the policy can gate on it.

- **What gets projected.** Each planned change gets an estimated monthly cost before and after, and the delta; the
  plan gets the total. Examples:
  - a `Service` from its size × count × hours (Fargate vCPU and memory) plus its load balancer (hours and LCUs);
  - a `Table` from its capacity mode and the observed reads and writes;
  - a `Bucket` from the observed storage and requests;
  - a `Queue` or `Topic` from the observed message volume.
- **Estimates come from deterministic code**, not the model. Each provider has a cost model that turns its spec and
  usage into billable units. The units are priced from the AWS Price List API (`pricing:GetProducts`), cached daily
  per region, with a bundled table as the fallback when the API isn't reachable.
- **Usage.** Usage comes from the env's CloudWatch metrics over the last 30 days. A new resource has no usage, so its
  estimate states its assumptions ("assumes 0 GB stored, 1M requests a month"). Each figure is marked `Exact`
  (fixed by the spec, e.g. Fargate tasks) or `Estimated` (usage-driven).
- **Where it shows:**
  - the plan text and approval requests (`~ update Service web [Low] +$43.20/month: 2 -> 4 tasks`);
  - `ExplainPlan`'s input;
  - a `costDelta` fact per change, plus the plan's total, so `POLICY.md` can say "anything adding more than
    $200 a month in prod needs an owner";
  - the change evidence (§6).
- **A deterministic guard:** an optional `costLimits` per env in `.iae/config.json`; a plan over its limit needs an
  owner whatever the policy text says.
- **Calibration:** the cost job (§5.10) compares the projections with Cost Explorer's actuals by the `iae:app` and
  `iae:env` tags, and reports where the estimates are off.
- **Testing:** Floci doesn't emulate the Price List API (to be confirmed), so `iae-testkit` gets a `PricingFake`
  with fixed prices, and the cost models are unit-tested against hand-worked bills.

*As built (M7):*
- **The estimate:** each `PlannedChange` carries a `CostDelta` (monthly USD before and after, `exact` or estimated,
  with its assumptions), filled in by the engine's `Pricing` after planning. An estimate that fails is left out
  and never fails a plan.
- **The plan hash leaves costs out,** since usage-based estimates drift between the agent's plan and the
  executor's.
- **`AwsPricing`** prices from a bundled us-east-1 `PriceBook` of public list prices:
  - a service: tasks (vCPU and GB × count × 730 h), the load balancer (hours plus an assumed one LCU) and its alarms
    on both colors. An update compares with the size the live version recorded (`iae:capacity` on the load
    balancer).
  - a secret: its monthly price. A role: nothing.
  - a queue, topic or table: 30 days of CloudWatch traffic (messages, publishes, consumed capacity) plus its
    default alarms.
  - a bucket: its stored bytes; requests aren't counted, since S3 request metrics are off by default.
  - a new resource has no usage yet, which its assumptions say.
- **Where it shows:** the plan text shows each change's cost (`+$43.20/month (est.)`) and the total ("cost: +$…
  a month (projected)"), so approval requests carry it. Facts carry `monthlyCost`.
- **The limit:** `costLimits` per env in `.iae/config.json` puts a change that adds more than the limit to an owner,
  whatever the policy text says.
- **Not yet:** the Price List API (the bundled book is the fallback the spec calls for), the plan-level limit (it
  is per change for now), and calibration against Cost Explorer.

**Verdict.** `EvaluatePolicy` uses zio-bedrock structured output (`.as[PolicyVerdict]`), one verdict per change:

```scala
enum Decision:
  case Auto
  case Approve(count: Int, groups: Set[SlackGroup])
  case Deny

final case class ChangeVerdict(
  change:    ChangeId,
  decision:  Decision,
  clauses:   List[String],   // the policy sentences it relied on, quoted
  reasoning: String,
  confident: Boolean,
) derives Schema
```

The engine then combines and checks it deterministically:

- **Most restrictive wins** across the plan's changes: `Deny` beats `Approve`, a larger count beats a smaller one,
  groups are combined.
- **Fail safe.** If the call fails, the output doesn't decode, a change has no verdict, `confident` is false,
  or a quoted clause isn't actually in `POLICY.md`, that change becomes `Approve(1, owners)` and the request says
  the policy was unclear. Groups must be Slack user groups listed in `.iae/config.json`; any other group becomes
  `owners`.
- **Built-in floor.** Code that the policy text can't override:
  - changes to `.iae/POLICY.md` need `Approve(1, owners)`, where `owners` is set at bootstrap;
  - changes to the agent's own deployment (§5.12) need `Approve(1, owners)`;
  - in prod, `Critical` changes need at least `Approve(1, owners)`;
  - the agent's own policy proposals are never `Auto`.
  The policy can only add to the floor.
- **Stable per plan.** The verdict is computed once per (policy text hash, change facts hash, UTC date of the facts)
  and kept in memory,
  so re-running a reconcile doesn't re-roll the dice. The executor (§5.11) has no model. It doesn't re-read the
  policy text, but recomputes the plan and the built-in floor and verifies the approval itself, so what the policy
  text alone requires rests on the agent's verdict, carried by the approval request it posted. The floor and human
  approvals don't rest on the agent.
- **Visible.** The approval request quotes the clauses and the reasoning, so a human can see *why* approval is
  needed, and the verdict, clauses and transcript go into evidence (§6). `iae policy explain` runs the same
  evaluation against a plan without applying anything.

**Testing the policy.** `.iae/policy-examples/<name>.json` holds one example change, `{"facts": <ChangeFacts>,
"expected": <Decision>}`. When a push changes `POLICY.md` or `.iae/config.json`, the agent decides every example
under the deployed and the proposed policy (with the fail-safes and floor, as in a plan) and adds the results to
the policy-change approval request, so the owners see how the new wording behaves before approving it. Examples
that don't get their expected decision are listed first, and examples whose decision changed are marked.

**Guarding the policy itself.** The policy used for an env is the text at the commit **currently deployed**
there, not the one being deployed: the newest `iae:commit` tag among the env's managed resources (§5.3). The
approval settings in `.iae/config.json` (owners, groups, production envs, identities) come from the same commit;
only the apps to build and the channel come from the target. A change can't loosen its own gate:

- When the target changes `POLICY.md` or `.iae/config.json`, every non-empty plan in that env carries a
  `PolicyText` change, which the floor gives to the owners, and the request shows the policy examples.
- The new policy takes effect once an approved plan has applied, because that moves the deployed commit. A push
  that changes only the policy has nothing to apply, so it takes effect with the next change it gates, which then
  needs the owners.
- Before an env's first apply there is no deployed commit, and the target's policy governs (bootstrap).

A typed policy DSL (`.iae/Policy.scala`) can replace or complement the text later. Since both produce the same
`ChangeVerdict`s, nothing downstream changes (open question Q16).

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
   Slack message from the iae bot: env, commit and PR, the change table with risks, the policy clauses and reasoning
   that require approval, the required group and count, the expiry, and a footer with the plan hash. If an open request for the
   same plan exists, the engine reuses it. Older requests for the env are edited to say "superseded".
2. The agent replies in the thread with a plain-language explanation: what changes, why, blast radius, how to roll
   back, and anything it's unsure about.
3. **A human approves by reacting ✅** (`:white_check_mark:`) on the request, or rejects with ❌. The agent receives the
   reaction over Slack Socket Mode (§5.7) and re-runs the env's reconcile.
4. The reconcile recomputes the plan and asks the engine whether it's approved.

**Request lifecycle.** A request's first line says where it stands, and the agent edits it as that changes (keeping
its metadata): `*Approval needed*` while open; `*Approved*` once the approved plan has applied; `*Rejected*`;
`*Superseded*` when a request for a different plan in the same env replaces it; `*Expired*`. Approved, superseded
and expired requests cover nothing. A rejected one keeps covering its plan, so the change stays stopped until a new
commit (or until the request expires and a new one is asked). An open request with no answer after the reminder
period (default 1h) gets one thread reply that mentions the required groups (`<!subteam^…>`).

The engine's checks, made with its own Slack token and never trusting the agent:

- The request message was posted by the iae bot user and its footer carries the plan hash.
- The recomputed plan is covered by the request: same target manifest hash, and its changes are a subset of the
  requested changes. A subset is allowed so a crashed apply can finish without re-approval, since the changes
  already made drop out of the recomputed plan. Anything outside the set (drift, a newer commit) makes the request
  stale and a new one is posted.
- `reactions.get` shows ✅ from at least `n` distinct users who are members of each required group
  (`usergroups.users.list`; a person in two groups counts for both), are human (not bots), and aren't the
  change's author (GitHub-to-Slack identity mapping in `.iae/config.json`). Any ❌ from a member of a required group
  rejects it. Because each group must supply the count, combining two changes' requirements into one request never
  weakens either: a plan that needs an owner for a policy change and an approver for a resource change needs both.
- Not expired (default 24h for prod; the policy text can say otherwise).
- Reuse is impossible without a record: once applied, the recomputed plan is empty and there's nothing left for
  the approval to cover.

Slack attributes each reaction to the real user who added it. So even though the agent can post through the iae
bot, it can't produce a human approval.

Evidence (§6) captures the request permalink, the approvers' Slack and mapped GitHub identities, reaction times
and the policy verdicts with their quoted clauses.

Alternatives considered:

- *Plan PRs* (approval file merged with CODEOWNERS review): strong GitHub-native trail, but slow and awkward for
  on-call. It could come back as an optional second channel for teams that live in GitHub.
- *Slack buttons*: nicer UX, and clicks would reach the agent through the webhook, but a click isn't recorded anywhere
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
| Approval request | engine | Structured plan, quoted policy clauses and reasoning, required group/count, expiry, plan hash | ✅ / ❌ reactions |
| Question | agent (`ask` tool) | The situation, the options, the agent's recommendation, and what happens with no answer | A threaded reply |
| Notice | engine or agent | Applied, deployed, canary step, verified, rolled back, failed, drift found, with links | Nothing |
| Report | cost / cleanup jobs | Periodic summary with proposals | Reactions or replies per proposal |
| Escalation | agent | Something that needs someone now; mentions the on-call group | A reply |

**When the agent asks for guidance.** The task prompts tell the agent to ask instead of guessing when: it isn't
confident of a fix; there's more than one reasonable fix with different trade-offs; a change it would author is
outside what the policy lets it do alone; it sees drift it didn't cause; or an alert's cause is unclear. People
can also start a conversation by mentioning the bot in the channel ("why did staging roll back?").

**Waiting.** A question parks the task: the conversation stays in memory and the agent does other work. A reply
arrives through the webhook and resumes the task with the reply appended. The default for no answer is always to
do nothing: after a reminder period (default 1h) the agent mentions the group, and when the wait expires it
posts that it stopped. After a restart, waits are restored from the thread (§5.3).

**Guidance isn't authorisation.** A reply can steer what the agent does or proposes ("use a new bucket instead of
renaming it"), but it never authorises an apply. Anything the policy gates still needs ✅ reactions on an engine
approval request. Replies count only from members of the configured responder group, and the task prompts treat
their text as information to weigh, not as instructions that override the policy.

**Identity and tokens.** One Slack app, "iae", with a bot token (post, update, read history and reactions, read
user groups) and the signing secret its events are checked with (webhook only), both in Secrets Manager in the
tooling account.

**Approval requests carry what they cover in Slack message metadata** (`iae_approval_request`: app, env, manifest
hash, plan hash, the change keys, required count and groups, author, expiry). The agent finds a covering request
by reading the channel's history, so nothing is stored outside Slack. A request covers a later plan with the same
manifest whose changes are a subset of the request's, which is how a crashed apply finishes without a new
approval. Eligible reactions come from members of the required groups who aren't bots, the change's author (mapped
from the commit email through `identities` in `.iae/config.json`) or the agent itself.

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
  manifest = iae manifest @ sha          # printed by the app in that build; kept per commit in the build bucket
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

**Builds.** The agent never runs the repo's code itself. For each app and commit it zips the commit
(`git archive`) into the tooling account's build bucket (`iae/sources/<sha>.zip`) and runs a CodeBuild build with the
app's build commands and manifest command from `.iae/config.json` (`apps[].build`, `apps[].manifest`, optionally
`apps[].image`). The manifest comes back as the build's artifact and is kept at `iae/manifests/<app>/<sha>.json`, so a
commit is built once however many envs and reconciles ask for it, including after a restart; concurrent requests
share one build. Each build gets a CodeBuild project of its own, created with everything the build needs and deleted
when it finishes, so nothing depends on `StartBuild` overrides (Floci ignores them). The builds' service role reaches
only the build bucket: builds run the repo's code and must not reach the env accounts. Without a build bucket
(`IAE_BUILD_BUCKET`) the agent runs the commands itself, which is for development only.

**Expand/contract ordering** keeps old and new app versions working during rollout: additive changes go before
the deploy and removals go after it. A failure at any step stops that env, rolls back the deploy if needed (infra
expand changes are left in place since they're additive), records evidence, and starts a `DiagnoseFailure` task (§5.4): the agent posts a diagnosis in the run thread
and either proposes a fix PR or asks for guidance.

### 5.9 Deploy and canary

- The build (CodeBuild, §5.8) produces a container image (e.g. sbt-native-packager Docker) pushed to ECR, tagged
  with the sha (see **Images** below).
- `Service` resource: ECS Fargate service with **two target groups** behind an ALB listener with weighted
  forwarding.
- Canary: start the new task set on the green target group, shift weight `0 → 10 → 50 → 100%` with a bake time at
  each step. CloudWatch alarms (5xx rate, latency p99, plus app-declared alarms in `Infra`) gate each step. If an
  alarm fires, weight goes back to 0 and the deploy is marked failed. All of this is deterministic engine code; the
  canary watcher is a fiber in the agent process, so a bake window is just a `ZIO.sleep` between checks.
- Staging gets the same flow with shorter bake times, so the canary machinery runs on every change.

**As built.** `Service.run(Id)` declares a `ServiceSpec`: port, health path, size, count, listener port, canary
steps, bake, alarm period, environment and command. Its image isn't in the code. The agent fills it in from the
app's `container` in `.iae/config.json` (`…/photo-app:${commit}`), so a new commit's image changes the service's
spec. That makes a deploy an ordinary `Update` of the `Service` resource, which goes through policy, approvals, the
executor and evidence like any other change.

The provider creates:

- an ECS cluster per app and env;
- an ALB;
- blue and green ip target groups;
- a listener with weighted forwarding;
- per color, the alarms that gate its canary (below), written as the color deploys.

Applying an `Update` deploys:

1. Register a task definition (task role: the app's runtime role).
2. Start the idle color's ECS service.
3. Wait for its tasks.
4. Move the weights through the steps, sleeping for the bake at each and reading the color's alarm.
5. Retire the old color.

The load balancer's tags (`iae:color`, `iae:image`, `iae:spec-hash`) record what's live, so there is still no
state file.

An alarm during a bake puts all traffic back on the old color and stops the new one. It also tags the spec as
failed (`iae:failed`), so the version isn't retried and re-planning it fails with "rolled back …; push a fix". That
keeps the env from converging, so later envs never get the version.

**Images.** An app either names its image (`container`) or sets `pushesImage`, and then its build pushes it:

- The agent keeps an ECR repository per app, `iae/<app>`, in the builds' (tooling) account.
- Its policy lets the env accounts in `accounts` pull from it.
- The build is given `IAE_IMAGE` (`<repository>:<commit>`) to push, e.g.
  `docker build -t "$IAE_IMAGE" . && docker push "$IAE_IMAGE"` (CodeBuild with privileged mode).
- A build that succeeds without pushing it fails ("succeeded but didn't push …").
- The app's services run `$IAE_IMAGE`.

Task definitions use the env account's `iae/iae-task-execution` role to pull images and write logs, which bootstrap
creates. In the harness, Floci's CodeBuild has no Docker daemon, so the tests push the image the build would have
pushed (`FlociRegistry`). ECS on Floci then runs it from Floci's ECR.

**The executor and long deploys.** The executor has a lane per env: requests run one at a time within an env, and
side by side across envs. So a canary baking in staging doesn't hold up a prod apply. A request that is already
waiting in its lane is dropped, since the agent asks again on each reconcile until the change is in. Without an
executor, the deploy holds only its own env's reconcile.

**Canary gates.** A step rolls back when any of the new color's alarms goes off during its bake:

- **Errors:** the load balancer's `HTTPCode_Target_5XX_Count` for the color reaches `maxErrors` (default 1) in a
  period.
- **Latency** (optional, `latency = LatencyGate(millis)`): the color's `TargetResponseTime` goes over `millis` for the
  statistic, `p99` by default (any CloudWatch statistic; percentiles go as extended statistics).
- **The app's own alarms** (`alarms = List(AppAlarm(name, metric, threshold))`): a metric the app publishes reaches
  the threshold, or with `below`, falls to it. Each version is judged on its own numbers:
  - the containers get `IAE_METRICS_NAMESPACE` (`iae/<app>`), `IAE_SERVICE` and `IAE_COLOR`;
  - the app publishes with the `Service` and `Color` dimensions;
  - declaring such alarms grants the runtime role `cloudwatch:PutMetricData`, with a condition that keeps it to
    the app's namespace (PutMetricData has no resource to scope to).
  - That grant widens the role, so the built-in floor asks for an approver when alarms are first declared, in
    every env. The harness scenarios approve the web service's first version for that reason.

A color's alarms are written as the color deploys, before its tasks start, and alarms the spec no longer has are
removed. A change to the gates therefore takes effect with the next version. A rollback names the alarm: "rolled
back …: its uploads alarm went off at 50%".

In the harness, the traffic generator publishes each color's 5xx count and response time, as an ALB would. Fault
controls make the version in canary answer 5xx, answer slowly or report failing uploads, per env. The `slow-deploy`
and `failing-metric` scenarios use them. Floci doesn't evaluate percentile statistics, so the example's latency
gate uses `Average`. Floci can also take more than one period to evaluate a rewritten alarm, so a bake that expects
an alarm should last at least one and a half periods.

### 5.10 Background jobs

Each job is a supervised fiber with its own `Schedule` and inbox subscription. Jobs find things
deterministically and hand judgement to an LLM task. Every change they want goes through the policy with the job
as `origin`, so the user decides how much each job may do alone.

**Reconcilers** (§5.8): one per env. Triggered by pushes, approvals, build results and drift, plus a periodic
full reconcile.

**Monitor.** Watches CloudWatch alarms for every managed app: the defaults (5xx rate, latency, queue age, DLQ
depth, throttles) and the alarms the app declares in `Infra`. Alarms open a `Triage` task (§7). The monitor also
does a periodic health sweep and posts a notice when an env degrades without an alarm firing.

*As built (M6):*
- **Sweep:** every minute (on the agent's clock), the monitor reads the alarms watching each app's live services in
  each env (`Engine.alarms`). These are the live color's canary gates (§5.9), which keep watching a version after
  its canary.
- **Alert:** an alarm going off raises a message in the channel: "*Alert* `app/env`: the web service's 5xx alarm
  (`<alarm>`) went off, running `<image>` from `<commit>`", mentioning the owners.
- **Triage:** the task runs in the alert's thread, away from the sweep.
- **Resolution:** when the alarm clears, the thread says so ("Cleared … after N minutes") and the message becomes
  "*Resolved*".
- **No memory needed:** open alerts are the agent's own alert messages in the channel, found again by the alarm
  they name, so a restart neither repeats nor loses one.
- **Default alarms on other kinds:**
  - every queue gets an age alarm (oldest message over 15 minutes);
  - with a dead-letter queue, a dead-letters alarm (any message in it);
  - every table gets read and write throttle alarms.

  They are written when the resource is applied and removed when it's deleted. They're kept out of the spec, so
  adding or tuning them changes no spec hash. Their alerts name the resource ("the jobs queue's dead letters
  alarm"). Only services can be rolled back.
- **Health without an alarm:** the watch reports each live service's health as an entry of its own. It's degraded
  when fewer tasks run than wanted, or when the load balancer finds targets unhealthy. The monitor raises it ("the
  web service is degraded without an alarm: 1 of 2 tasks running") once it lasts two sweeps, since task counts dip
  while ECS replaces a task, and resolves it when healthy again.

**Drift.** AWS Config and CloudTrail events on `iae:managed` resources from a principal other than the deployer
role mean someone changed infra by hand. The agent posts what changed and who did it, then plans a revert to the
manifest. The revert is a normal plan, gated by policy (`origin = Drift`), with the drift described in the
approval request. If the hand change should stay, the answer is a PR changing the `Infra` declaration.

*As built (M6):*
- **Detection:** the reconcile finds drift by itself, deterministically. A planned update to a resource whose
  recorded spec (`iae:spec-hash`) is already the desired one changed outside the pipeline, so its provider flags it
  `Drift`. A pending new spec, or a service's version waiting for a fix after a rollback, isn't drift.
- **Judging:** a drifted change's facts carry `origin = Drift` and the `drift` flag, so `POLICY.md` can treat
  reverts on their own terms.
- **Reporting:** the agent posts "*Drift* `app/env`: changed outside the pipeline …" once per plan, with the changed
  fields. If the revert needs approval, the request says it's a revert ("*Drift:* Bucket uploads changed outside
  the pipeline; this reverts it to the manifest").
- **Who did it:**
  - The engine's attribution looks up the resource's write calls in CloudTrail's event history (`LookupEvents` on
    its physical name, the last 7 days). It leaves out reads and the pipeline's own deployer role.
  - The drift notice names who changed each resource and when ("PutBucketVersioning by
    `arn:aws:sts::…:assumed-role/Admin/carol` at 10:30 UTC"), or says CloudTrail doesn't have it.
  - The planner role needs `cloudtrail:LookupEvents`.
  - Floci records only S3 data events and Secrets Manager writes, so the tests use `CloudTrailFake`.
- **Harness:** the `drift` scenario reverts a hand change in staging. In `drift-in-prod`, the revert of a prod
  hand change waits for an approver.

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

**Security auditor.** Continuous, plus a weekly report. Sources, all scoped to `iae:managed` resources:

- IAM Access Analyzer: unused permissions on the generated roles, and external or public access;
- Security Hub and GuardDuty findings;
- Inspector scans of the app images in ECR;
- configuration checks the engine can run itself (encryption, public access, logging, TLS on listeners).

`ReviewSecurity` ranks the findings, explains them in Slack, and fixes what it can as **PRs**: narrow a capability
in the app code (`Bucket` to `BucketReader`), turn on a setting in the `Infra` declaration, or bump a base image or
dependency and rebuild. The PRs go through the normal pipeline and policy (`origin = Security`). Active threats
(a GuardDuty finding of compromised credentials, for example) are escalations, not PRs.

### 5.11 Credentials and trust boundary

The agent runs in its own **tooling account** on ECS Fargate with a task role. There are no long-lived AWS keys.

- **The agent's own credentials** come from its ECS task role (the container credentials endpoint), or static keys in
  development. It assumes the env roles with STS AssumeRole and refreshes their credentials five minutes before they
  expire. `.iae/config.json` maps each env to its account: `accounts.<env>` with the `planner` and `deployer` role ARNs
  and an optional `region`. An env without one uses the agent's own account (a single-account setup). The engine
  plans through the planner role and applies through the deployer role.
- **Planner role** in each env account (read-only, plus CloudWatch, Cost Explorer and security-findings reads). The agent process
  assumes it for observing, planning, monitoring and cost.
- **Deployer role** in each env account (writes, limited to `iae:managed` resources, with a permissions
  boundary, no IAM wildcard).
- **Phase 1:** the agent process can assume the deployer role, and only the engine's apply path does so, after
  the policy and approval checks. The LLM has no tool that reaches it.
- **Phase 2 (before prod):** a separate **executor** service (`iae-executor`, same engine code, no LLM, its own
  task role and Slack app) is the only principal that can assume deployer roles. The agent sends (env, app, commit,
  phase, plan hash, approval request) to the executor's SQS queue (`IAE_EXECUTOR_QUEUE_URL`) and plans with the
  planner roles only. Before applying, the executor:
  - checks out the commit itself, which must be on the branch;
  - recomputes the plan, which must have the same hash;
  - reads the author from git;
  - recomputes the policy deployed in the env and the built-in floor;
  - verifies the approval request with its own Slack token: posted by the agent's bot, open, unexpired, for this
    app, env and manifest, and covering every change;
  - verifies the reactions against the stricter of the floor and the request.

  It then applies, posts the notice, labels the request, records the evidence and wakes the agent through its
  inbox. A refusal is posted once. A compromised agent process can then propose but not mutate. It can at most
  under-state what the policy text alone requires, never the floor or a human approval.

Other credentials, in Secrets Manager in the tooling account:

- **GitHub App** "iae" (`IAE_GITHUB_APP_ID`, `IAE_GITHUB_INSTALLATION_ID`, and `IAE_GITHUB_APP_KEY` or
  `IAE_GITHUB_APP_KEY_FILE`): private key and webhook secret. The agent signs app JWTs (RS256) with the key, and
  keeps installation tokens until five minutes before they expire. Installation tokens for cloning, branches, PRs, statuses,
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
- Locally, `./sbta agent/Test/run` runs the agent in the harness described in §4.1.

---

## 6. Audit Manager integration

`iae bootstrap` creates, per env account:

- A custom framework **"IAE Change Management"** with custom controls (manual evidence sources):
  - `IAE-CM-1` change originates from a reviewed commit on `main`
  - `IAE-CM-2` automated tests passed for that commit
  - `IAE-CM-3` plan approved as required by policy: policy commit, verdicts with quoted clauses and the
    `EvaluatePolicy` transcript, Slack request permalink,
    approvers and reaction times (or auto-approved by policy commit X)
  - `IAE-CM-4` deployment verified (health and canary results)
  - `IAE-CM-5` exceptions: break-glass, rollbacks, failed applies, drift reverts
- An assessment over that framework scoped to the env account, plus optionally a standard framework (such as SOC 2)
  so CloudTrail/Config automated evidence about the same resources sits next to ours.
- An evidence bucket `iae-evidence-<env>` with Object Lock (compliance mode) and versioning.

`.iae/config.json` points each env at its evidence: `accounts.<env>.audit` with the bucket, the assessment, the
control set and the id of each IAE control. Evidence is written with the env's deployer role to
`iae/evidence/<env>/<app>/<sha>/<plan>-<phase>/<control>.json`. A failure to record evidence is reported, and the
apply it describes stands. So far the agent records `IAE-CM-1` and `IAE-CM-3` for every apply; `IAE-CM-2`,
`IAE-CM-4` and `IAE-CM-5` come with builds' tests, deploys and break-glass.

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
- The agent keeps the alert thread open until the alarm clears, then posts a summary.

*As built (M6):*
- **Tools:** `Triage` is a zio-bedrock task with read-only tools:
  - `alarms`: every alarm on the app's live services in the env, and whether it fires;
  - `deployment`: the live image and commit, and the branch's last 15 commits with times and authors;
  - the repository at the live commit (`listFiles`, `readFile`, `search`);
  - with a GitHub App, `writeFile` and `openPullRequest`, for a fix PR as in `DiagnoseFailure`.
- **Findings:** it writes what's happening, the likely cause with evidence, a recommendation (rollback, code fix,
  `Infra` change, or a person's investigation) and how sure it is.
- **Without a model:** the alert still mentions the owners, but no triage runs.
- **Rollback (`rollback` tool):** asks for the alerting service to go back to the version it ran before.
  - **The plan:** a canary deploy records the version it replaced (`iae:previous-image`, `iae:previous-spec-hash` on
    the load balancer). `Engine.rollback` plans one `Rollback` change from it; there is none when nothing is
    recorded.
  - **The policy:** the plan is judged by the policy deployed in the env, as `origin = Alert` with the agent as its
    author. It's `Low` risk (the previous version was live and approved), so the default table lets it go ahead;
    `POLICY.md` can ask for approvers.
  - **The apply:** in process, or through the executor, which plans the rollback again from the env, checks that
    the request's commit is the one deployed there, and verifies the floor and approvals as for any plan.
  - **Waiting on approvers:** the monitor asks again on each sweep and reports the outcome in the alert's thread.
  - **What the provider does:**
    - restarts the previous color (scaled to zero after the canary, with its task definition kept);
    - moves all the traffic back and stops the live color;
    - marks the live spec failed (`iae:failed`), so the reconcile asks for a fix ("push a fix") instead of
      redeploying it.
  - **The alert:** it resolves once its version no longer has the traffic ("Cleared: its version no longer has
    the traffic").
  - **Harness:** a fault on the live version is pinned to that version, so it stays with it after a rollback. The
    `incident-rollback` scenario has v2 fail in prod and rolled back through the executor, and the alert resolves
    although v2 is still broken.
- **Harness:** fault controls also target the live version, and the `incident` scenario raises and resolves an
  alert in prod. It never mutates prod
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
  modules/agent/src/test/     # AppTest local harness: FakeSlack + web UI, FakeGitHub, scenarios (§4.1)
  examples/
    photo-app/                 # InfraApp using Bucket + Queue; harness seed; fault endpoint + issue patches
  .github/workflows/           # CI for this repo (build + Floci tests)
```

A repo managed by zio-iae has:

```
.iae/config.json               # apps (build commands, manifest command, build image), envs (promotion order),
                               #   accounts (planner/deployer roles, region) per env,
                               #   Slack channel, responder group, GitHub-to-Slack identity mapping
.iae/POLICY.md                 # the user's approval policy, plain English (§5.5)
.iae/policy-examples/*.json    # example changes with expected decisions
CODEOWNERS                     # policy owners for .iae/POLICY.md
```

and the "iae" GitHub App installed.

---

## 9. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 ✅ | Project bootstrap via start.jamesward.com (`AGENTS.md`, sbt 2, CI, sbta) | CI green on an empty module set |
| M1 ✅ | `iae-core` model; `iae-aws-client` SigV4 + S3; `Bucket` capability + provider; `FlociLayer`; manifest | Example app reads and writes a bucket on Floci; provider convergence tests pass |
| M2 ✅ | Engine: plan, change facts, verdict combination + built-in floor, idempotent apply, commit guard, tag discovery; `MessageQueue`, `Secret`, `Table`, `Topic`; IAM generation; `iae` CLI | `assertConverges` end-to-end on Floci for the example app, including crash/re-run |
| M3 (in progress) | Agent process: webhook + SQS inbox ✅, per-env reconcilers ✅, manifests built in CodeBuild ✅, Slack approval requests + verification, notices, reminders, expiry and superseding ✅, rehydrate on restart ✅, `EvaluatePolicy` with `POLICY.md` (zio-bedrock structured output, tested with `ScriptedModel`; risk table without a model) + policy examples ✅, policy from the deployed commit ✅; env accounts through assumed planner and deployer roles ✅; local harness ✅ (`AppTest`, `FakeSlack` + web UI, push panel, a Floci per account, builds in Floci's CodeBuild, real model when configured, clock control, scenarios run from the UI and headlessly in `ScenarioSpec`); staging account | In the harness: a push converges staging, a gated change waits for ✅ from the right user, and an agent restart loses no approval ✅. Then the same on a real staging account |
| M4 (in progress) | zio-bedrock tasks: `ExplainPlan` ✅, `DiagnoseFailure` with fix PRs through a GitHub App ✅, `Converse` with read-only tools ✅; agent-authored PRs (fix PRs ✅; merging its own PRs when the policy allows to follow); `ScriptedModel` tests ✅; Audit Manager evidence (IAE-CM-1 and IAE-CM-3 per apply ✅); executor split (deterministic: plan, floor and approvals re-checked) ✅; prod account | A failing build produces a diagnosis and a fix PR; a prod change with approval produces evidence for IAE-CM-1..4 |
| M5 (in progress) | `Service` + ALB canary + auto-rollback ✅; builds that push images to ECR ✅; per-env executor lanes ✅; latency and app-declared canary alarms ✅; harness traffic generator ✅, fault controls ✅ and the `bad-deploy`, `slow-deploy` and `failing-metric` scenarios ✅ | The "bad deploy" scenario rolls back automatically, in the harness and in staging |
| M6 ✅ (locally) | Monitor ✅ (service gates, queue and table default alarms, health without an alarm) + drift reporting with CloudTrail attribution and policy-gated revert ✅; `Triage` task ✅ with policy-gated rollback through the executor ✅ | A synthetic alarm results in a rollback or a fix PR; a hand change is reported and reverted under policy |
| M7 (in progress) | Cleanup, cost optimizer and security auditor jobs; cost projection on plans (§5.5.1) ✅ (bundled prices; Price List API to follow) | Orphans and old images are proposed for cleanup; a weekly cost report with at least one right-sizing PR; an unused permission becomes a PR narrowing the capability |

---

## 10. Open questions

| # | Question | Current leaning |
|---|---|---|
| Q1 | Package and artifact naming: `zio.iae` / `dev.zio`-style vs `com.jamesward` | `zio.iae` package, `com.jamesward` org, unless it moves under the ZIO org |
| Q2 | Resource identity via singleton-type `Tag`s: verify izumi-reflect stability; fallback to marker traits | Resolved in M1: works across modules (§3.2) |
| Q3 | Hand-written `iae-aws-client` vs zio-aws vs Java SDK v2 | Hand-written for the ops we need (ZIO-native streams, Scala 3, small footprint); revisit if the surface grows |
| Q4 | Manifest extraction: running `main --iae-manifest` via sbt per commit is slow | Done in CodeBuild alongside the build and kept per commit in the build bucket (§5.8) ✅; later an sbt task that only links the `infra` value |
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
| Q16 | When to add a typed policy DSL alongside or instead of `POLICY.md` | When the examples show the text giving inconsistent verdicts, or when a team wants rules checked at compile time. Same `ChangeVerdict` output, so it's additive |
| Q17 | Consistency of LLM verdicts across model versions | Pin the model per policy evaluation; re-run `.iae/policy-examples` on a model change and treat differences like a policy change |
| Q18 | One Floci container per simulated account vs. one shared container (does Floci separate accounts by credentials?) | Per account to start: simplest isolation; revisit if startup time hurts |
| Q19 | Floci CodeBuild for local builds (real Docker, slow) vs. a local build runner | Floci CodeBuild, so the build path matches production; add a local runner behind the same service only if it's too slow |
| Q20 | Which git hosts to support (§5.2.1) | GitHub first (built); CodeCommit as the second host, behind the same inbox event and a `PullRequests` implementation per host, once the GitHub path runs on real accounts |
| Q21 | Price source for cost projection (§5.5.1): the Price List API, a bundled table, or both | Both: the Price List API cached daily, with a bundled table for when it's unreachable and for tests; usage from 30 days of CloudWatch metrics |

---

## 11. Next step

Bootstrap the repository with the instructions at <https://start.jamesward.com> (the `zen-of-projects` Skill):
sbt 2, `AGENTS.md`, CI, sbta. Then lay out the modules in §8 and start M1.
