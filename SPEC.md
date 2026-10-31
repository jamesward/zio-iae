# zio-iae: Infrastructure as Effects

Status: **Draft v0.1** (2026-10-31)

zio-iae makes cloud infrastructure part of a ZIO program's type. An app that needs an S3 bucket asks for one in its
environment (`ZIO[Bucket[Uploads], ...]`) the same way it asks for any other service. The infrastructure is
declared once and used for three things:

1. **At runtime** it provides the ZIO layers the program uses (`Bucket`, `Queue`, ...).
2. **At deploy time** it is the desired-state description that an engine reconciles against AWS.
3. **For security** it generates the app's least-privilege IAM policy from the capabilities the program uses.

A Claude Code scheduled task (routine) runs the pipeline. It reacts to changes on `main`, plans and applies
infrastructure changes (including changes it authors itself), deploys the app (later with canaries), and records
evidence in AWS Audit Manager. **The user configures a policy** that says which kinds of changes need a human
approval. The agent talks to people in **Slack**: it asks for approvals there, asks for guidance when it's unsure,
and reports what it did. Later the same agent takes production alerts, triages them and proposes
fixes, which then go through the same pipeline.

The project has two parts:

| Part | What it is | Who uses it |
|---|---|---|
| **Library** (`iae-core`, `iae-aws`, `iae-aws-client`, `iae-testkit`) | Resource traits, the infra description model, AWS providers, a thin ZIO AWS client, Floci test layers | App developers, in their ZIO programs |
| **Agent** (`iae-engine`, `iae-agent`, routine prompt, Slack relay) | Deterministic plan/apply/deploy engine exposed as a CLI and MCP server, the Claude Code routine that drives it, and the Slack integration | The routine; humans approving and guiding in Slack |

---

## 1. Goals and non-goals

### Goals

- **Infra is a typed effect requirement.** Using a resource the app hasn't declared is a compile error, and a
  declared resource that is never used shows up as a warning in the plan.
- **One declaration, three uses:** runtime layer, desired state, IAM policy.
- **Capability-scoped traits** (`BucketReader` vs `Bucket`) that map directly to IAM actions.
- **Agent-managed delivery.** Every merge to `main` converges each environment to `main` HEAD with no human
  steps unless policy calls for an approval.
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
  table, file or database of its own to decide what to do (§5.8).
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
capability: the app *is* the service. See §5.7.

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
  also the concurrency guard (§5.8).
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
| Engine end-to-end | manifest, plan, apply, re-plan gives an empty plan (`assertConverges`); crash mid-apply then re-run converges; two concurrent runs at different commits converge to the newer one | Floci |
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
| Observe, plan, classify risk, evaluate the user's policy | Explain plans in plain language in the run thread |
| Post approval requests to Slack; verify approvals through Slack's API | Ask humans for guidance in Slack when it's unsure, and act on their answers |
| Apply idempotently with the commit guard, re-observe and resume | Author changes (code, `Infra` declarations, config) as PRs; merge them only as policy allows (§5.3) |
| Deploy, canary, auto-rollback on alarm | Diagnose failed builds, tests, applies and canaries; propose fixes |
| Write evidence to S3 + Audit Manager | Summarise outcomes in Slack; later: triage alerts |
| **Only component holding AWS write credentials** | **Never calls mutating AWS APIs directly** |

The engine is a published application (Scala Next) with two faces:

- CLI: `iae manifest | plan | apply | deploy | promote | rollback | drift | status | bootstrap`
- MCP server: `iae --mcp`, the same operations as structured tools that return JSON (plan with risks, policy
  verdict, per-change apply results), so the agent doesn't parse logs.

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
| **Slack activity** | Slack Events API, `iae-slack-relay` Lambda, `/fire` with `slack <channel> <thread_ts>` | A human reacted to an approval request or replied in a run thread (§5.5). |
| Alerts (phase 3) | CloudWatch alarm, SNS, a small Lambda, `/fire` with the alarm summary | Triage, see §7. |

**Runs are level-triggered.** An event is only a wake-up. Every run reconciles each env toward the *current*
`main` HEAD, not the event's sha. Dropped, duplicated or out-of-order events are harmless, and concurrent sessions
converge through idempotent, commit-guarded changes (§5.8). Fire payloads arrive as untrusted text, so the routine prompt uses them only as hints
and re-derives everything from git and AWS.

### 5.3 Approval policy (user-configured)

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
`dataLoss`); **origin** (`Human` for commits authored by people, `Agent` for commits the agent authored, `Alert` for
changes made while handling an alert); time windows.

Evaluation:

- Every change is evaluated. The plan's verdict is the **most restrictive** across all matching rules (and the
  default for changes no rule matches): `Deny` beats `Approve`, a larger `n` beats a smaller one, and the required
  groups are combined. Order doesn't matter, so adding a rule can only tighten things.
- Each verdict carries the rules that produced it, so the approval request can say *why* approval is needed.
- `iae policy explain` evaluates the policy against a plan without applying anything, and `iae-testkit` has
  helpers to unit-test a policy against sample plans.

Guarding the policy itself:

- The policy used for an env is the one at the commit **currently deployed** there (read from the `iae:commit`
  tags, §5.8), not the one being deployed. A change can't loosen its own gate.
- Any change to `.iae/Policy.scala` is itself a change the engine plans (`PolicyChange`), and a built-in rule that
  the user can't override requires `Approve(1, owners)` for it, where `owners` is set at bootstrap.
- The agent may propose policy changes but they never count as `Auto`.

Agent-authored changes. The agent can change infra by opening PRs (a fix, a tuning change, a new resource). For an
agent PR the engine previews the plan for each env at the PR head and evaluates policy with `origin = Agent`:

- If every env's verdict is `Auto`, the agent may merge its own PR. This needs a GitHub ruleset that lets the
  agent's identity merge with a required status check `iae/policy` that the engine sets. Human code review
  requirements stay whatever the user configured in GitHub.
- Otherwise the agent asks for approval in Slack before merging. Approvals bind to the manifest and change set,
  not the commit (§5.4), so an approval given on the PR preview still covers the run after merge as long as the
  plan hasn't changed.

A stateful resource removed from the manifest isn't deleted by default. It's **orphaned**: tagged
`iae:orphaned=<date>`, left alone, and reported. Deletion requires the resource to be declared with
`Retention.Delete` (or an explicit `iae destroy <id>`), and is then `Critical`.

### 5.4 Approvals

When a plan's verdict is `Approve(n, groups)`:

1. The engine posts an **approval request** in the run thread for (app, env, target manifest). It is a structured
   Slack message from the iae bot: env, commit and PR, the change table with risks, the policy rules that require
   approval, the required group and count, the expiry, and a footer with the plan hash. If an open request for the
   same plan exists, the engine reuses it. Older requests for the env are edited to say "superseded".
2. The agent replies in the thread with a plain-language explanation: what changes, why, blast radius, how to roll
   back, and anything it's unsure about.
3. **A human approves by reacting ✅** (`:white_check_mark:`) on the request, or rejects with ❌. The Slack relay
   wakes the routine (§5.5).
4. The next run recomputes the plan and asks the engine whether it's approved.

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
- *Slack buttons*: nicer UX, but a click isn't recorded anywhere in Slack. The relay would have to record it, as a
  reply from a separate approvals app whose token only the relay holds. Planned as v2 on top of reactions.
- *GitHub Environments with required reviewers*: only gates GitHub Actions jobs. It fits if applies move into
  Actions (§5.9, option B variant).

Break-glass: a human with admin AWS access can run `iae apply --break-glass --reason ...` locally. The engine
posts it to the channel and records it as an exception in Audit Manager.

### 5.5 Slack: the agent's communication channel

**Layout.** One channel per app (`#iae-<app>`, configured in `.iae/config.json`), one **run thread** per
(env, target manifest). Everything about one rollout happens in its thread, so a new session can rebuild the
whole conversation by reading the thread.

**Message kinds:**

| Kind | Posted by | Content | Expects |
|---|---|---|---|
| Approval request | engine | Structured plan, matched rules, required group/count, expiry, plan hash | ✅ / ❌ reactions |
| Question | agent (through the engine's MCP tools) | The situation, the options, the agent's recommendation, and what happens with no answer | A threaded reply |
| Notice | engine or agent | Applied, deployed, canary step, verified, rolled back, failed, with links | Nothing |
| Escalation | agent | A question or failure that needs someone now; mentions the on-call group | A reply |

**When the agent asks for guidance.** The routine prompt tells the agent to ask instead of guessing when:
a build, test or apply fails and it isn't confident of the fix; there's more than one reasonable fix with
different trade-offs; a change it would author is outside what the policy lets it do alone; it sees drift it
didn't cause; or an alert's cause is unclear.

**Asking doesn't block.** The agent posts the question and the session ends. When someone replies, the relay fires
the routine, and the new session reads the thread and continues. The default for no answer is always to do
nothing: after a reminder period (default 1h) the agent mentions the group, and when the request expires it
posts that it stopped.

**Guidance isn't authorisation.** A reply can steer what the agent does or proposes ("use a new bucket instead of
renaming it"), but it never authorises an apply. Anything the policy gates still needs ✅ reactions on an engine
approval request. Replies are accepted only from members of the configured responder group, and the routine
prompt treats their text as information to weigh, not as instructions that override the policy or the prompt.

**Waking the agent.** Scheduled tasks can't subscribe to Slack directly, so `iae bootstrap` provisions
`iae-slack-relay`: a Lambda with a Function URL registered as the Slack app's Events API endpoint. It verifies
Slack's request signature, keeps only relevant events (`reaction_added` on iae bot messages, human `message`
replies in run threads), and POSTs `slack <channel> <thread_ts>` to the routine's `/fire`. The payload is only a
pointer: the session re-reads everything from Slack and AWS. The hourly reconcile also re-checks open approval
requests in case an event was dropped.

**Identities and tokens.** One Slack app, "iae", with a bot token (post, update, read history and reactions,
read user groups) and a signing secret (the relay only). The agent uses Slack only through the engine's MCP tools
(`slack_ask`, `slack_notify`, `slack_read_thread`), so messages look the same and go through one code path.

**No state.** Slack is where the conversation and the approvals live, and they're read back through Slack's API,
the same way infra is read back through AWS's. Nothing is copied into a store of ours. Evidence copies go to S3
(write-only, §6). Slack message retention for the channel has to be at least the approval expiry (see open
question Q11).

### 5.6 Pipeline for one reconcile run

```
for env in [staging, prod]:              # promotion order from .iae/config.json
  sha      = main HEAD
  if env != first and previous env isn't observed running sha verified: continue   # promotion gate (§5.8)
  build + test at sha                    # sbt Test/compile; test (Floci); skipped if the sha's image is in ECR
  manifest = iae manifest @ sha
  plan     = observe + diff (expand phase only: creates/updates, no deletes)
  verdict  = policy(env, plan)
    Deny    -> report, stop env
    Approve -> approved? continue : (post or reuse the Slack approval request; stop env, resumes on ✅)
    Auto or approved ->
      apply(plan)                        # idempotent, re-observe + commit guard before each change
      deploy(app image @ sha)            # phase 2, §5.7
      verify                             # health + alarms over a bake window
      contract phase: deletes/orphaning (a separate plan; usually Approve in prod)
  evidence(env, sha, plan, approval, results)
  GitHub Deployment status for (env, sha)   # display only, never read back
```

**Expand/contract ordering** keeps old and new app versions working during rollout: additive changes go before
the deploy and removals go after it. A failure at any step stops that env, rolls back the deploy if needed (infra
expand changes are left in place since they're additive), records evidence, and the agent posts a diagnosis in the run thread, and either proposes a fix PR or asks for
guidance.

### 5.7 Deploy and canary (phase 2)

- The build produces a container image (sbt-native-packager Docker) pushed to the env's ECR, tagged with the sha.
- `Service` resource: ECS Fargate service with **two target groups** behind an ALB listener with weighted
  forwarding.
- Canary: start the new task set on the green target group, shift weight `0 → 10 → 50 → 100%` with a bake time at
  each step. CloudWatch alarms (5xx rate, latency p99, plus app-declared alarms in `Infra`) gate each step. If an
  alarm fires, weight goes back to 0 and the deploy is marked failed. All of this is done by the engine, not the
  agent.
- Staging gets the same flow with shorter bake times, so the canary machinery runs on every change.

### 5.8 State: none of our own

The engine has no state file, table or database. Every decision is made from two inputs: the repo at `main` HEAD
(desired state, policy) and what the cloud APIs report now (observed state). Approvals and guidance are read
from Slack's API in the same way. Things that usually need
a state store are derived instead:

| Need | Derived from |
|---|---|
| What we own | Tags (`iae:app`, `iae:env`, `iae:id`) via the Tagging API and list calls (§3.6) |
| Whether a resource is current | `iae:spec-hash` and `iae:commit` tags on the resource |
| Crash recovery | Nothing to resume: every `apply` is idempotent, so a re-run re-observes and the remaining changes are simply the new plan (covered by the approval as a subset, §5.4) |
| Approvals | Reactions on the engine's approval request, read through Slack's API (§5.4) |
| Approval already used | The recomputed plan is empty once applied (§5.4) |
| What is deployed | ECS service, its task definition and deployments (image tag = sha), ALB listener weights |
| Canary progress | Observed listener weights and ECS deployment rollout state, so an interrupted canary resumes from where the cloud says it is |
| Promotion gate | The previous env's service is observed running the sha, tagged `iae:verified=<sha>` after its bake window passed |
| Already built | Image for the sha exists in ECR |
| History | CloudTrail, ECS deployment history, the evidence bucket. These are **write-only** for the engine: evidence is never read to make a decision. |

**Concurrency without locks.** Two sessions can run at once (the routine starts one per event). Correctness comes
from:

- **Commit guard:** before changing a resource, the engine re-reads its `iae:commit` tag. If that commit is a
  descendant of the one this run targets, the resource is newer and the run stops for that env. So an older run
  can never roll back a newer one, and two runs at the same commit make the same idempotent changes.
- **Re-observe before each change**, so a run never acts on a stale read for longer than one API call.
- **Serialisation by infrastructure** once applies move to the in-account executor (§5.9, option B): the executor
  is a CodeBuild project with a concurrent build limit of 1 per env. That's configuration of a provisioned
  resource, not a state store.

The remaining race window (between the guard's read and the write) is accepted for MVP. It's covered by the
level-triggered design: the next run sees the result and converges.

### 5.9 Credentials and trust boundary

Routine sessions are ephemeral cloud containers with env vars set per environment, so whatever credentials the
engine uses are visible to anyone who can use that environment.

- **Option A (MVP):** the routine environment holds credentials for an `iae-deployer` role per env, and the
  Slack bot token. The role can
  only touch `iae:managed` resources and has a permissions boundary; it has no IAM wildcard. The engine enforces
  policy and approvals. Risk: a compromised session could bypass the engine with the same credentials.
- **Option B (target):** the routine holds only an `iae-planner` read role plus permission to start an **in-account
  apply executor** (Step Functions or CodeBuild running the same engine). The executor re-verifies the approval
  against Slack with its own token and holds the write role. The agent can then propose and observe but can't mutate on its own. A
  variant runs the executor in GitHub Actions with OIDC and Environments.

We start with A against non-prod accounts and move to B before managing prod. See open question Q5.

---

## 6. Audit Manager integration

`iae bootstrap` creates, per env account:

- A custom framework **"IAE Change Management"** with custom controls (manual evidence sources):
  - `IAE-CM-1` change originates from a reviewed commit on `main`
  - `IAE-CM-2` automated tests passed for that commit
  - `IAE-CM-3` plan approved as required by policy: matched rules, policy commit, Slack request permalink,
    approvers and reaction times (or auto-approved by policy commit X)
  - `IAE-CM-4` deployment verified (health and canary results)
  - `IAE-CM-5` exceptions: break-glass, rollbacks, failed applies
- An assessment over that framework scoped to the env account, plus optionally a standard framework (such as SOC 2)
  so CloudTrail/Config automated evidence about the same resources sits next to ours.
- An evidence bucket `iae-evidence-<env>` with Object Lock (compliance mode) and versioning.
- The `iae-slack-relay` Lambda (§5.5), with the Slack signing secret in Secrets Manager.

For each reconcile of an env, the engine writes one JSON evidence document per control to the evidence bucket
(commit, PR + reviewers, test summary, plan hash + changes, approval refs, per-change apply results, CloudTrail event IDs,
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
  1. **Recent deploy correlates:** the engine runs `iae rollback <env>` if policy allows it for
     `origin = Alert` (it returns to a state that was already approved); otherwise it asks in Slack.
  2. **Code fix:** the agent opens a PR with a fix and a regression test, linking the alert. It goes through
     review and the normal pipeline, with canary.
  3. **Infra tuning** (such as queue visibility or concurrency): a PR changing the `Infra` declaration, so the change
     is reviewed like any other.
  4. **Unknown:** the agent posts its triage notes as an escalation in Slack and asks for guidance.
- The agent never mutates prod outside the pipeline. Rollback is the only fast path, and it's policy-gated.
  Each alert gets its own Slack thread, linked from the run threads of any changes it leads to.

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
    ROUTINE.md                 # the scheduled task's prompt (when to ask in Slack, how to treat replies)
    setup.sh                   # environment setup script: installs iae, registers MCP
  modules/slack-relay/         # the Slack Events relay Lambda
  examples/
    photo-app/                 # InfraApp using Bucket + Queue; end-to-end on Floci
  .github/workflows/           # CI (build + Floci tests); optional iae-fire.yml
```

A repo using zio-iae has:

```
.iae/config.json               # app entry points, envs + accounts/regions, promotion order,
                               #   Slack channel, responder group, GitHub-to-Slack identity mapping
.iae/Policy.scala              # the user's approval policy (§5.3)
CODEOWNERS                     # policy owners for .iae/Policy.scala
```

---

## 9. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | Project bootstrap via start.jamesward.com (`AGENTS.md`, sbt 2, CI, sbta) | CI green on an empty module set |
| M1 | `iae-core` model; `iae-aws-client` SigV4 + S3; `Bucket` capability + provider; `FlociLayer`; manifest | Example app reads and writes a bucket on Floci; provider convergence tests pass |
| M2 | Engine: plan, policy DSL + evaluation + `iae policy explain`, idempotent apply, commit guard, tag discovery; `Queue`, `Table`, `Secret`, `Topic`; IAM generation | `assertConverges` end-to-end on Floci for the example app, including crash/re-run and concurrent runs |
| M3 | Routine + MCP server; merge trigger; Slack app, relay, approval requests + verification, questions and notices; staging account | A merged PR that changes infra converges staging; a change the policy gates waits for ✅ in Slack; the agent asks a question, gets a reply and continues in a new session |
| M4 | Audit Manager bootstrap + evidence; prod account; credentials option B; agent-authored PRs merged under policy | Prod change with approval produces evidence for IAE-CM-1..4 |
| M5 | `Service` + ECR + ALB canary + auto-rollback | Bad build in staging is rolled back automatically by an alarm |
| M6 | Alert trigger + triage tools | Synthetic alarm results in a rollback or a fix PR |

---

## 10. Open questions

| # | Question | Current leaning |
|---|---|---|
| Q1 | Package and artifact naming: `zio.iae` / `dev.zio`-style vs `com.jamesward` | `zio.iae` package, `com.jamesward` org, unless it moves under the ZIO org |
| Q2 | Resource identity via singleton-type `Tag`s: verify izumi-reflect stability; fallback to marker traits | Spike in M1 |
| Q3 | Hand-written `iae-aws-client` vs zio-aws vs Java SDK v2 | Hand-written for the ~40 ops we need (ZIO-native streams, Scala 3, small footprint); revisit if the surface grows |
| Q4 | Manifest extraction: running `main --iae-manifest` via sbt per commit is slow | Reuse within a session only (no persisted cache); later an sbt task that only links the `infra` value |
| Q5 | Move to credentials option B before prod? Executor in Step Functions/CodeBuild vs GitHub Actions + OIDC | B before prod; pick executor in M4 |
| Q6 | Engine's GitHub token for evidence (PR reviewers for IAE-CM-1) and the `iae/policy` status check | Fine-grained token in the env, or moves into the executor (B) |
| Q7 | Multi-app repos and shared infra (VPC, ALB, DNS zone) owned by a platform `InfraModule` | Platform module with its own Slack channel and policy; apps reference it by id |
| Q8 | Multi-region / multi-account per env | Out of scope until M5 |
| Q9 | Scheduled-task event caps and research-preview churn | Level-triggered design + hourly reconcile makes dropped events harmless; keep the routine prompt thin so the engine holds the logic |
| Q10 | Is the commit guard's small read-then-write race acceptable before option B serialises applies? | Yes for non-prod; prod waits for B |
| Q11 | Slack retention and audit: approvals must stay readable for at least their expiry; long-term proof is the S3 evidence copy | Require channel retention ≥ 30 days; document it in bootstrap |
| Q12 | Use Claude in Slack (Claude Tag) for the conversational side instead of relay + routine? | Start with relay + routine so the engine and policy stay in one place; revisit once approvals are stable |
| Q13 | Reactions (v1) vs buttons with a separate approvals app (v2) | Reactions first: no extra endpoint state, unforgeable attribution |
| Q14 | Mapping GitHub authors to Slack users for the "author can't approve" rule | Explicit mapping in `.iae/config.json`; fall back to email match through Slack's `users.lookupByEmail` |

---

## 11. Next step

Bootstrap the repository with the instructions at <https://start.jamesward.com> (the `zen-of-projects` Skill):
sbt 2, `AGENTS.md`, CI, sbta. Then lay out the modules in §8 and start M1.
