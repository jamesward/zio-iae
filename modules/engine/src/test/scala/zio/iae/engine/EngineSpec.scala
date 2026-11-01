package zio.iae.engine

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.test.*

object EngineSpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val naming = BucketNaming(None)

  object Uploads extends Bucket.Id(LogicalId("uploads"), BucketSpec(versioning = Versioning.Enabled))
  object Thumbs  extends Bucket.Id(LogicalId("thumbs"))
  object Logs    extends Bucket.Id(LogicalId("logs"))

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

  def engineWith(history: CommitOrder): URIO[S3Client & TaggingClient, Engine] =
    for
      s3      <- ZIO.service[S3Client]
      tagging <- ZIO.service[TaggingClient]
    yield Engine.make(AwsEngine.drivers(s3, naming), TaggingDiscovery(tagging), history)

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
        plan.changes.map(_.change.productPrefix) == List("Create", "Create", "Create"),
        plan.changes.forall(_.risk == Risk.Low),
        commit.contains(sha('a')),
      )
    ,
    test("a crash part-way through an apply converges on the next run"):
      for
        env       <- uniqueEnv
        s3        <- ZIO.service[S3Client]
        tagging   <- ZIO.service[TaggingClient]
        manifest  <- manifestOf(everything)
        remaining <- Ref.make(1)
        crashing   = Engine.make(Drivers(List(CrashingDriver(BucketDriver(BucketProvider(s3, naming)), remaining))), TaggingDiscovery(tagging), LinearHistory(Nil))
        crashed   <- crashing.converge(manifest, env, None).either
        engine    <- engineWith(LinearHistory(Nil))
        leftOver  <- engine.plan(manifest, env, None, Phase.Expand)
        result    <- assertConverges(engine, manifest, env, None)
      yield result && assertTrue(crashed.isLeft, leftOver.changes.size == 2)
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
        readopt.changes.flatMap:
          _.change match
            case Change.Update(_, _, fields) => fields.toList.map(_.field)
            case _                           => Nil
        .forall(_ == "orphaned"),
        readopt.changes.size == 2,
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
  ).provideShared(
    FlociLayer.live,
    Client.default,
    S3Client.layer,
    AwsJsonClient.layer,
    TaggingClient.layer,
  ) @@ TestAspect.withLiveRandom @@ TestAspect.withLiveClock @@ TestAspect.timeout(3.minutes)
