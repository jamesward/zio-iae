package zio.iae.aws

import java.nio.charset.StandardCharsets.UTF_8
import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.client.*
import zio.iae.testkit.FlociLayer
import zio.stream.ZStream
import zio.schema.{Schema, derived}
import zio.test.*

/** Integration tests against Floci. Each test uses its own env name, so tests share one container safely. */
object FlociSpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val naming = BucketNaming(None)

  object Uploads extends Bucket.Id(LogicalId("uploads"))

  final case class Job(photo: String) derives Schema, CanEqual

  object DatabasePassword extends Secret.Id(LogicalId("database-password"))

  final case class PhotoId(value: String) derives Schema, CanEqual
  final case class PhotoInfo(width: Int, height: Int, tags: List[String]) derives Schema, CanEqual
  object PhotoInfos extends Table.Id[PhotoId, PhotoInfo](LogicalId("photo-info"))

  object Thumbnails extends MessageQueue.Id[Job](LogicalId("thumbnails"))
  object Uploaded   extends Topic.Id[Job](LogicalId("uploaded"), List(Topic.to(Thumbnails)))
  object Jobs extends MessageQueue.Id[Job](LogicalId("jobs"), QueueSpec(visibilityTimeout = 1.second, deadLetter = Some(DeadLetter(maxReceives = 3))))
  object Archive extends Bucket.Id(LogicalId("archive"), BucketSpec(versioning = Versioning.Enabled))

  val uniqueEnv: UIO[EnvName] =
    ZIO
      .collectAll(List.fill(8)(Random.nextIntBounded(26).map(i => ('a' + i).toChar)))
      .map(cs => EnvName.from("t" + cs.mkString).toOption.get)

  def key(value: String): ObjectKey = ObjectKey.from(value).toOption.get

  def bytes(text: String): ZStream[Any, Nothing, Byte] = ZStream.fromChunk(Chunk.fromArray(text.getBytes(UTF_8)))

  def cloud(env: EnvName, mode: ProvisionMode): URIO[AwsCloud.Clients, Cloud] =
    AwsCloud.layer(app, env, mode, naming).build.provideSome[AwsCloud.Clients](Scope.default).map(_.get)

  def spec = suite("Floci")(
    suite("BucketProvider")(
      test("create, then observe, then nothing left to change"):
        for
          env      <- uniqueEnv
          provider <- ZIO.serviceWith[S3Client](BucketProvider(_, naming))
          address   = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
          desired   = BucketSpec(versioning = Versioning.Enabled)
          before   <- provider.observe(address)
          create    = provider.diff(address, desired, before)
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local))
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local)) // re-running is safe
          after    <- provider.observe(address)
        yield assertTrue(
          before.isEmpty,
          create.exists(_.isInstanceOf[Change.Create[?]]),
          after.exists(_.versioning == VersioningStatus.Enabled),
          provider.diff(address, desired, after).isEmpty,
        )
      ,
      test("a lifecycle is set, shown as a change, changed, and removed"):
        for
          env      <- uniqueEnv
          provider <- ZIO.serviceWith[S3Client](BucketProvider(_, naming))
          address   = Address(app, env, ResourceKind.Bucket, LogicalId("logs"))
          tiered    = BucketSpec(lifecycle = Some(Lifecycle(tieringAfterDays = Some(0), expireAfterDays = Some(365))))
          _        <- ZIO.foreachDiscard(provider.diff(address, tiered, None))(provider.applyChange(_, ApplyContext.local))
          set      <- provider.observe(address)
          shorter   = BucketSpec(lifecycle = Some(Lifecycle(expireAfterDays = Some(30))))
          change    = provider.diff(address, shorter, set)
          _        <- ZIO.foreachDiscard(change)(provider.applyChange(_, ApplyContext.local))
          changed  <- provider.observe(address)
          _        <- ZIO.foreachDiscard(provider.diff(address, BucketSpec(), changed))(provider.applyChange(_, ApplyContext.local))
          removed  <- provider.observe(address)
        yield assertTrue(
          set.map(_.lifecycle).contains(Lifecycle(Some(0), Some(365))),
          provider.diff(address, tiered, set).isEmpty,
          change.toList.flatMap { case Change.Update(_, _, fields) => fields.toList; case _ => Nil }.exists(f =>
            f.field == "lifecycle" && f.from == "Intelligent-Tiering after 0 days, expire after 365 days" && f.to == "expire after 30 days"),
          changed.map(_.lifecycle).contains(Lifecycle(expireAfterDays = Some(30))),
          removed.map(_.lifecycle).contains(Lifecycle()),
        )
      ,
      test("a spec change converges and keeps tags that aren't ours"):
        for
          env      <- uniqueEnv
          s3       <- ZIO.service[S3Client]
          provider  = BucketProvider(s3, naming)
          address   = Address(app, env, ResourceKind.Bucket, LogicalId("uploads"))
          _        <- ZIO.foreachDiscard(provider.diff(address, BucketSpec(), None))(provider.applyChange(_, ApplyContext.local))
          name     <- ZIO.fromEither(naming.physical(address))
          tags     <- s3.getBucketTagging(name)
          _        <- s3.putBucketTagging(name, tags + ("team" -> "photos"))
          desired   = BucketSpec(versioning = Versioning.Enabled)
          observed <- provider.observe(address)
          update    = provider.diff(address, desired, observed)
          _        <- ZIO.foreachDiscard(update)(provider.applyChange(_, ApplyContext.local))
          after    <- provider.observe(address)
        yield assertTrue(
          update.exists(_.isInstanceOf[Change.Update[?]]),
          provider.diff(address, desired, after).isEmpty,
          after.exists(_.tags.get("team").contains("photos")),
        )
      ,
    ),
    suite("RoleProvider")(
      test("a role converges, widening is flagged, narrowing isn't, and discovery finds it"):
        val read  = RoleSpec(List(Grant(LogicalId("uploads"), ResourceKind.Bucket, Capability.BucketRead)), List(TrustedService.EcsTasks))
        val write = read.copy(grants = read.grants :+ Grant(LogicalId("uploads"), ResourceKind.Bucket, Capability.BucketWrite))
        for
          env      <- uniqueEnv
          iam      <- ZIO.service[IamClient]
          context  <- ZIO.service[AwsContext]
          provider  = RoleProvider(iam, naming, context)
          address   = Address(app, env, ResourceKind.Role, Manifest.runtimeRole)
          create    = provider.diff(address, read, None)
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local))
          created  <- provider.observe(address)
          widen     = provider.diff(address, write, created)
          _        <- ZIO.foreachDiscard(widen)(provider.applyChange(_, ApplyContext.local))
          widened  <- provider.observe(address)
          narrow    = provider.diff(address, read, widened)
          found    <- RoleDiscovery(iam).managed(app, env)
        yield assertTrue(
          create.exists(c => provider.assess(c, None).flags.contains(ChangeFlag.IamWidening)),
          provider.diff(address, read, created).isEmpty,
          widen.exists(c => provider.assess(c, created) == Assessment(Risk.High, Set(ChangeFlag.IamWidening))),
          provider.diff(address, write, widened).isEmpty,
          narrow.exists(c => provider.assess(c, widened) == Assessment(Risk.Medium, Set.empty)),
          found.map(_.address) == List(address),
        )
      ,
    ),
    suite("QueueProvider")(
      test("a queue with a dead-letter queue converges, then changes settings and drops the redrive"):
        val desired = QueueSpec(visibilityTimeout = 45.seconds, deadLetter = Some(DeadLetter(maxReceives = 5)))
        val changed = QueueSpec(visibilityTimeout = 90.seconds)
        for
          env      <- uniqueEnv
          provider <- ZIO.serviceWith[SqsClient](QueueProvider(_))
          address   = Address(app, env, ResourceKind.Queue, LogicalId("jobs"))
          create    = provider.diff(address, desired, None)
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local))
          _        <- ZIO.foreachDiscard(create)(provider.applyChange(_, ApplyContext.local))
          created  <- provider.observe(address)
          update    = provider.diff(address, changed, created)
          _        <- ZIO.foreachDiscard(update)(provider.applyChange(_, ApplyContext.local))
          after    <- provider.observe(address)
        yield assertTrue(
          created.exists(_.deadLetterExists),
          created.flatMap(_.attributes.get(QueueProvider.Redrive)).flatMap(QueueProvider.maxReceives).contains(5),
          provider.diff(address, desired, created).isEmpty,
          update.exists:
            case Change.Update(_, _, fields) => fields.map(_.field).toSet == Set("visibility timeout (s)", "dead-letter max receives", s"tag ${TagKey.Spec.wire}")
            case _                           => false
          ,
          provider.diff(address, changed, after).isEmpty,
        )
      ,
      test("typed messages go through SQS, and unacknowledged ones come back"):
        for
          env     <- uniqueEnv
          c       <- cloud(env, ProvisionMode.AutoProvision)
          jobs    <- c.queue(Jobs)
          _       <- jobs.send(Job("a.jpg"))
          first   <- jobs.receive(max = 10, wait = 2.seconds)
          _       <- ZIO.sleep(1500.millis)
          again   <- jobs.receive(max = 10, wait = 2.seconds)
          _       <- ZIO.foreachDiscard(again)(r => jobs.ack(r.receipt))
          _       <- ZIO.sleep(1500.millis)
          gone    <- jobs.receive(max = 10, wait = 1.second)
          bindOk  <- cloud(env, ProvisionMode.BindOnly).flatMap(_.queue(Jobs)).either
        yield assertTrue(
          first.map(_.message) == Chunk(Job("a.jpg")),
          again.map(r => (r.message, r.receiveCount)) == Chunk((Job("a.jpg"), 2)),
          gone.isEmpty,
          bindOk.isRight,
        )
      ,
    ),
    suite("TopicProvider")(
      test("a published message reaches the subscribed queue, and dropping the subscriber unsubscribes it"):
        for
          env      <- uniqueEnv
          c        <- cloud(env, ProvisionMode.AutoProvision)
          queue    <- c.queue(Thumbnails)
          topic    <- c.topic(Uploaded)
          _        <- topic.publish(Job("cat.jpg"))
          received <- queue.receive(max = 10, wait = 5.seconds)
          sns      <- ZIO.service[SnsClient]
          sqs      <- ZIO.service[SqsClient]
          context  <- ZIO.service[AwsContext]
          provider  = TopicProvider(sns, sqs, context)
          address   = Address(app, env, ResourceKind.Topic, Uploaded.logicalId)
          observed <- provider.observe(address)
          dropped   = provider.diff(address, TopicSpec(Nil), observed)
          _        <- ZIO.foreachDiscard(dropped)(provider.applyChange(_, ApplyContext.local))
          after    <- provider.observe(address)
          policy   <- sqs.queueUrl(s"photo-app-${env.value}-thumbnails").someOrFail(AwsError.InvalidRequest("no queue")).flatMap(sqs.attributes).map(_.get("Policy"))
        yield assertTrue(
          received.map(_.message) == Chunk(Job("cat.jpg")),
          provider.diff(address, Uploaded.spec, observed).isEmpty,
          dropped.exists:
            case Change.Update(_, _, fields) => fields.exists(f => f.field == "subscription" && f.to == "(none)")
            case _                           => false
          ,
          after.exists(_.subscriptions.isEmpty),
          policy.exists(_.contains("sns.amazonaws.com")),
        )
      ,
    ),
    suite("TableProvider")(
      test("a table converges, and typed items go in and come out"):
        for
          env      <- uniqueEnv
          provider <- ZIO.serviceWith[DynamoClient](TableProvider(_))
          address   = Address(app, env, ResourceKind.Table, PhotoInfos.logicalId)
          c        <- cloud(env, ProvisionMode.AutoProvision)
          photos   <- c.table(PhotoInfos)
          observed <- provider.observe(address)
          _        <- photos.put(PhotoId("cat"), PhotoInfo(640, 480, List("cute")))
          found    <- photos.get(PhotoId("cat"))
          missing  <- photos.get(PhotoId("dog"))
          _        <- photos.delete(PhotoId("cat"))
          gone     <- photos.get(PhotoId("cat"))
          bound    <- cloud(env, ProvisionMode.BindOnly).flatMap(_.table(PhotoInfos)).either
        yield assertTrue(
          observed.exists(_.table.status == "ACTIVE"),
          provider.diff(address, PhotoInfos.spec, observed).isEmpty,
          found.contains(PhotoInfo(640, 480, List("cute"))),
          missing.isEmpty,
          gone.isEmpty,
          bound.isRight,
        )
      ,
    ),
    suite("SecretProvider")(
      test("a secret is created without a value, tagged, and readable once someone sets it"):
        for
          env      <- uniqueEnv
          secrets  <- ZIO.service[SecretsClient]
          provider  = SecretProvider(secrets)
          address   = Address(app, env, ResourceKind.Secret, DatabasePassword.logicalId)
          c        <- cloud(env, ProvisionMode.AutoProvision)
          reader   <- c.secret[DatabasePassword.type](DatabasePassword.logicalId, DatabasePassword.spec)
          before   <- reader.read.either
          _        <- secrets.putValue(SecretNaming.physical(address), "hunter2")
          after    <- reader.read
          observed <- provider.observe(address)
          changed   = provider.diff(address, SecretSpec("rotated monthly"), observed)
        yield assertTrue(
          before == Left(SecretError.NotSet(DatabasePassword.logicalId)),
          after.value == "hunter2",
          after.toString == "SecretValue(***)",
          provider.diff(address, DatabasePassword.spec, observed).isEmpty,
          changed.exists(_.isInstanceOf[Change.Update[?]]),
        )
      ,
    ),
    suite("AwsCloud")(
      test("binding fails until the bucket is provisioned, then binds"):
        for
          env    <- uniqueEnv
          bind   <- cloud(env, ProvisionMode.BindOnly)
          auto   <- cloud(env, ProvisionMode.AutoProvision)
          before <- bind.bucket[Uploads.type](Uploads.logicalId, Uploads.spec).either
          _      <- auto.bucket[Uploads.type](Uploads.logicalId, Uploads.spec)
          after  <- bind.bucket[Uploads.type](Uploads.logicalId, Uploads.spec).either
        yield assertTrue(
          before match
            case Left(InfraError.NotProvisioned(address)) => address.id == Uploads.logicalId
            case _                                        => false
          ,
          after.isRight,
        )
      ,
      test("binding to a bucket provisioned from another spec is a mismatch"):
        for
          env    <- uniqueEnv
          auto   <- cloud(env, ProvisionMode.AutoProvision)
          bind   <- cloud(env, ProvisionMode.BindOnly)
          _      <- auto.bucket[Uploads.type](Uploads.logicalId, BucketSpec())
          result <- bind.bucket[Uploads.type](Uploads.logicalId, BucketSpec(versioning = Versioning.Enabled)).either
        yield assertTrue(result match
          case Left(InfraError.SpecMismatch(_, _, actual)) => actual.isDefined
          case _                                           => false
        )
      ,
    ),
    suite("Bucket capability")(
      test("an assumed role's credentials are fetched from STS and used"):
        for
          base     <- ZIO.service[AwsConfig]
          http     <- ZIO.service[Client]
          iam      <- ZIO.service[IamClient]
          suffix   <- Random.nextIntBounded(1000000)
          name      = RoleName.from(s"iae-deployer-$suffix").toOption.get
          account  <- StsClient(AwsQueryClient(http, base)).accountId
          trust     = s"""{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::${account.value}:root"},"Action":"sts:AssumeRole"}]}"""
          _        <- iam.createRole(name, "/", trust, Map.empty)
          assumed  <- StsClient.assumed(base, http, s"arn:aws:iam::${account.value}:role/$name", "iae-agent")
          creds    <- assumed.credentials.credentials
          caller   <- StsClient(AwsQueryClient(http, assumed)).accountId
        yield assertTrue(creds.sessionToken.isDefined, caller == account)
      ,
      test("a program uses the buckets it declares"):
        val infra = Bucket.readWrite(Uploads) ++ Bucket.reader(Archive)
        val program =
          for
            uploads <- ZIO.service[Bucket[Uploads.type]]
            archive <- ZIO.service[BucketReader[Archive.type]]
            _       <- uploads.put(key("photos/a.jpg"), bytes("first"), Some("image/jpeg"))
            _       <- uploads.put(key("photos/b.jpg"), bytes("second"))
            _       <- uploads.put(key("notes/c.txt"), bytes("third"))
            body    <- uploads.get(key("photos/a.jpg")).runCollect.map(c => String(c.toArray, UTF_8))
            meta    <- uploads.head(key("photos/b.jpg"))
            photos  <- uploads.list(Some(key("photos/"))).runCollect
            _       <- uploads.delete(key("photos/a.jpg"))
            missing <- uploads.get(key("photos/a.jpg")).runCollect.either
            gone    <- uploads.head(key("photos/a.jpg"))
            empty   <- archive.list().runCollect
          yield assertTrue(
            body == "first",
            meta.exists(_.size == 6L),
            photos.map(_.key.value).toList == List("photos/a.jpg", "photos/b.jpg"),
            missing == Left(BucketError.NotFound(key("photos/a.jpg"))),
            gone.isEmpty,
            empty.isEmpty,
          )
        for
          env    <- uniqueEnv
          c      <- cloud(env, ProvisionMode.AutoProvision)
          result <- program.provideLayer(ZLayer.succeed(c) >>> infra.layer)
        yield result
      ,
    ),
  ).provideShared(FlociLayer.live, Client.default, AwsCloud.clients, AwsQueryClient.layer, IamClient.layer) @@ TestAspect.withLiveRandom @@ TestAspect.withLiveClock
