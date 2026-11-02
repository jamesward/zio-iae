package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.schema.{Schema, derived}
import zio.test.*

object EngineSpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val naming = BucketNaming(None)

  object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
  object Thumbs  extends Bucket.Id(LogicalId("thumbs"))
  object Logs    extends Bucket.Id(LogicalId("logs"))

  final case class Job(photo: String) derives Schema, CanEqual
  object Jobs extends MessageQueue.Id[Job](LogicalId("jobs"), QueueSpec(deadLetter = Some(DeadLetter(maxReceives = 5))))

  /** The same logical bucket as `Thumbs`, at an older commit that had a different spec. */
  object ThumbsVersioned extends Bucket.Id(LogicalId("thumbs"), BucketSpec(versioning = Versioning.Enabled))

  def manifestOf(infra: Infra[?]): IO[EngineError, Manifest] =
    ZIO.fromEither(infra.manifest(app)).mapError(EngineError.InvalidManifest(_))

  val everything = Bucket.readWrite(Uploads) ++ Bucket.readWrite(Thumbs) ++ Bucket.reader(Logs)

  def sha(c: Char): CommitSha = CommitSha.from(c.toString * 40).toOption.get

  /** A linear history: each commit is an ancestor of the ones after it. */
  final case class LinearHistory(commits: List[CommitSha]) extends CommitOrder:
    def isAncestor(ancestor: CommitSha, descendant: CommitSha): IO[EngineError, Boolean] =
      ZIO.succeed(commits.indexOf(ancestor) <= commits.indexOf(descendant))

  /** Wraps a driver so its applies start failing after `succeed` of them: a crash part-way through an apply. */
  final case class CrashingDriver(underlying: ResourceDriver, remaining: Ref[Int]) extends ResourceDriver:
    def kind                                    = underlying.kind
    def plan(address: Address, desired: ResourceSpec) = underlying.plan(address, desired)
    def lastCommit(address: Address)            = underlying.lastCommit(address)
    def apply(change: Change[ResourceSpec], context: ApplyContext) =
      remaining.getAndUpdate(_ - 1).flatMap: left =>
        if left > 0 then underlying.apply(change, context)
        else ZIO.fail(ProviderError(change.address, "apply", "simulated crash"))

  val uniqueEnv: UIO[EnvName] =
    ZIO.collectAll(List.fill(8)(Random.nextIntBounded(26).map(i => ('a' + i).toChar))).map(cs => EnvName.from("e" + cs.mkString).toOption.get)

  def engineWith(history: CommitOrder): URIO[AwsEngine.Clients, Engine] =
    AwsEngine.layer(naming).build.provideSome[AwsEngine.Clients](ZLayer.succeed(history), Scope.default).map(_.get)

  /** After converging, planning again finds nothing to do in either phase. */
  def assertConverges(engine: Engine, manifest: Manifest, env: EnvName, target: Option[CommitSha]): IO[EngineError, TestResult] =
    for
      _        <- engine.converge(manifest, env, target)
      expand   <- engine.plan(manifest, env, target, Phase.Expand)
      contract <- engine.plan(manifest, env, target, Phase.Contract)
    yield assertTrue(expand.isEmpty, contract.isEmpty)

  def spec = suite("Engine on Floci")(
    test("a fresh env converges, and the plan says what it will create"):
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(List(sha('a'))))
        manifest <- manifestOf(everything)
        plan     <- engine.plan(manifest, env, Some(sha('a')), Phase.Expand)
        result   <- assertConverges(engine, manifest, env, Some(sha('a')))
        commit   <- engine.planner.drivers(ResourceKind.Bucket).flatMap(_.lastCommit(Address(app, env, ResourceKind.Bucket, Uploads.logicalId)).mapError(EngineError.Provider(_)))
      yield result && assertTrue(
        plan.changes.map(c => (c.change.productPrefix, c.address.kind)) ==
          List(("Create", ResourceKind.Bucket), ("Create", ResourceKind.Bucket), ("Create", ResourceKind.Bucket), ("Create", ResourceKind.Role)),
        plan.changes.forall(_.risk == Risk.Low),
        plan.changes.filter(_.address.kind == ResourceKind.Role).forall(_.flags == Set(ChangeFlag.IamWidening)),
        commit.contains(sha('a')),
      )
    ,
    test("a crash part-way through an apply converges on the next run"):
      for
        env       <- uniqueEnv
        s3        <- ZIO.service[S3Client]
        tagging   <- ZIO.service[TaggingClient]
        iam       <- ZIO.service[IamClient]
        manifest  <- manifestOf(everything)
        remaining <- Ref.make(1)
        normal    <- engineWith(LinearHistory(Nil))
        crashing   = Engine.make(Drivers(normal.planner.drivers.all.map(CrashingDriver(_, remaining))), AwsEngine.discovery(tagging, iam), LinearHistory(Nil))
        crashed   <- crashing.converge(manifest, env, None).either
        engine    <- engineWith(LinearHistory(Nil))
        leftOver  <- engine.plan(manifest, env, None, Phase.Expand)
        result    <- assertConverges(engine, manifest, env, None)
      yield result && assertTrue(crashed.isLeft, leftOver.changes.size == 3)
    ,
    test("resources that are no longer declared are orphaned, not deleted, and can be adopted again"):
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(Nil))
        all      <- manifestOf(everything)
        fewer    <- manifestOf(Bucket.readWrite(Uploads))
        _        <- engine.converge(all, env, None)
        contract <- engine.plan(fewer, env, None, Phase.Contract)
        orphaned <- assertConverges(engine, fewer, env, None)
        s3       <- ZIO.service[S3Client]
        logs     <- ZIO.fromEither(naming.physical(Address(app, env, ResourceKind.Bucket, Logs.logicalId))).mapError(EngineError.InvalidManifest(_))
        stillThere <- s3.bucketExists(logs).mapError(e => EngineError.Provider(ProviderError(Address(app, env, ResourceKind.Bucket, Logs.logicalId), "check", e.message)))
        readopt  <- engine.plan(all, env, None, Phase.Expand)
        adopted  <- assertConverges(engine, all, env, None)
      yield orphaned && adopted && assertTrue(
        contract.changes.map(_.address.id) == List(Logs.logicalId, Thumbs.logicalId),
        contract.changes.forall(_.change.productPrefix == "Orphan"),
        stillThere,
        readopt.changes.filter(_.address.kind == ResourceKind.Bucket).flatMap:
          _.change match
            case Change.Update(_, _, fields) => fields.toList.map(_.field)
            case _                           => Nil
        .forall(_ == "orphaned"),
        readopt.changes.count(_.address.kind == ResourceKind.Bucket) == 2,
        readopt.changes.exists(c => c.address.kind == ResourceKind.Role && c.flags.contains(ChangeFlag.IamWidening)),
      )
    ,
    test("an app no repo declares is found, its cleanup orphans what's left of it, and later deletes it"):
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(Nil))
        all      <- manifestOf(Bucket.readWrite(Uploads))
        _        <- engine.converge(all, env, None)
        apps     <- engine.apps(env)
        today    <- Clock.localDateTime.map(_.toLocalDate.nn)
        early    <- engine.cleanup(app, env, today.minusDays(14).nn)
        orphan   <- engine.cleanup(app, env, today.minusDays(14).nn, abandoned = true)
        _        <- engine.apply(orphan)
        waiting  <- engine.cleanup(app, env, today.minusDays(14).nn, abandoned = true)
        expired  <- engine.cleanup(app, env, today, abandoned = true)
      yield assertTrue(
        apps.contains(app),
        early.isEmpty,
        orphan.changes.map(c => (c.address.kind, c.change.productPrefix)).toSet == Set(ResourceKind.Bucket -> "Orphan", ResourceKind.Role -> "Orphan"),
        orphan.changes.forall(_.risk == Risk.Low),
        waiting.isEmpty,
        expired.changes.map(c => (c.address.kind, c.change.productPrefix)).toSet == Set(ResourceKind.Bucket -> "Delete", ResourceKind.Role -> "Delete"),
      )
    ,
    test("an older commit can't undo a change made by a newer one"):
      val history = LinearHistory(List(sha('a'), sha('b')))
      for
        env     <- uniqueEnv
        engine  <- engineWith(history)
        v1      <- manifestOf(Bucket.readWrite(Thumbs))
        _       <- engine.converge(v1, env, Some(sha('b')))
        older   <- manifestOf(Bucket.readWrite(Uploads) ++ Bucket.readWrite(ThumbsVersioned))
        plan    <- engine.plan(older, env, Some(sha('a')), Phase.Expand)
        guarded <- engine.apply(plan).either
      yield assertTrue(guarded match
        case Left(EngineError.Guarded(address, resource, target)) =>
          address.id == Thumbs.logicalId && resource == sha('b') && target == sha('a')
        case _ => false
      )
    ,
    test("an app with a bucket, a queue and a role converges, and the role may only use its own queue"):
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(Nil))
        manifest <- manifestOf(Bucket.reader(Uploads) ++ MessageQueue.sender(Jobs))
        result   <- assertConverges(engine, manifest, env, None)
        iam      <- ZIO.service[IamClient]
        context  <- ZIO.service[AwsContext]
        role     <- ZIO.fromEither(RoleProvider.roleName(app, env)).mapError(e => EngineError.CommitOrder(e))
        policy   <- iam.getInlinePolicy(role, RoleProvider.policyName).mapError(e => EngineError.CommitOrder(e.message))
        actions   = policy.flatMap(IamPolicy.parsePermissions).fold(Set.empty[(String, String)])(_.permissions)
      yield result && assertTrue(
        actions.contains("sqs:SendMessage" -> QueueNaming.arn(context, s"photo-app-${env.value}-jobs")),
        !actions.exists(_._1 == "sqs:ReceiveMessage"),
        !actions.exists(_._1 == "s3:PutObject"),
      )
    ,
    test("queues are created before the topics that deliver to them, and the app converges"):
      object Fanout extends Topic.Id[Job](LogicalId("fanout"), List(Topic.to(Jobs)))
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(Nil))
        manifest <- manifestOf(Topic.publisher(Fanout) ++ MessageQueue.receiver(Jobs))
        plan     <- engine.plan(manifest, env, None, Phase.Expand)
        result   <- assertConverges(engine, manifest, env, None)
      yield result && assertTrue(plan.changes.map(_.address.kind) == List(ResourceKind.Queue, ResourceKind.Topic, ResourceKind.Role))
    ,
    test("plans are data: they round-trip through JSON and hash by content"):
      for
        env      <- uniqueEnv
        engine   <- engineWith(LinearHistory(Nil))
        manifest <- manifestOf(everything)
        plan     <- engine.plan(manifest, env, None, Phase.Expand)
        again    <- engine.plan(manifest, env, None, Phase.Expand)
        decoded   = zio.schema.codec.JsonCodec.jsonDecoder(zio.schema.Schema[Plan]).decodeJson(plan.toJson)
      yield assertTrue(decoded == Right(plan), plan.hash == again.hash)
    ,
  ).provideShared(FlociLayer.live, Client.default, AwsEngine.clients) @@ TestAspect.withLiveRandom @@ TestAspect.withLiveClock @@ TestAspect.timeout(3.minutes)
