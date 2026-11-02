package zio.iae.aws

import zio.iae.*
import zio.test.*

object IamPolicySpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val env    = EnvName("prod")
  val naming  = BucketNaming(None)
  val context = AwsContext(zio.iae.aws.client.Region.UsEast1, zio.iae.aws.client.AccountId.from("123456789012").toOption.get)

  def grant(id: String, capability: Capability) = Grant(LogicalId.from(id).toOption.get, ResourceKind.Bucket, capability)

  def spec = suite("IamPolicy")(
    test("a service with alarms of its own may publish metrics to its app's namespace only"):
      val publish = Grant(LogicalId("web"), ResourceKind.Service, Capability.MetricsPublish)
      val doc     = IamPolicy.permissions(app, env, RoleSpec(List(publish, grant("uploads", Capability.BucketRead)), Nil), naming, context)
      val json    = doc.map(_.toJson).getOrElse("")
      assertTrue(
        doc.map(_.statements.head.condition) == Right(Some(Map("StringEquals" -> Map("cloudwatch:namespace" -> "iae/photo-app")))),
        json.contains(""""Condition":{"StringEquals":{"cloudwatch:namespace":"iae/photo-app"}}"""),
        json.split("\"Condition\"").length == 2, // the bucket's statement has none, not a null one
      )
    ,
    test("a reader may get and list one bucket, nothing else"):
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(grant("uploads", Capability.BucketRead)), Nil), naming, context)
      assertTrue(
        doc.map(_.permissions) == Right(
          Set(
            "s3:GetObject"  -> "arn:aws:s3:::photo-app-prod-uploads",
            "s3:GetObject"  -> "arn:aws:s3:::photo-app-prod-uploads/*",
            "s3:ListBucket" -> "arn:aws:s3:::photo-app-prod-uploads",
            "s3:ListBucket" -> "arn:aws:s3:::photo-app-prod-uploads/*",
          )
        )
      )
    ,
    test("a writer may also put and delete objects, but not touch the bucket itself"):
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(grant("uploads", Capability.BucketWrite)), Nil), naming, context)
      assertTrue(
        doc.map(_.permissions) == Right(
          Set("s3:DeleteObject" -> "arn:aws:s3:::photo-app-prod-uploads/*", "s3:PutObject" -> "arn:aws:s3:::photo-app-prod-uploads/*")
        ),
        doc.map(_.statements.map(_.sid)) == Right(List("BucketWriteUploads")),
      )
    ,
    test("queue senders and receivers get only their side of one queue"):
      val send    = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("jobs"), ResourceKind.Queue, Capability.QueueSend)), Nil), naming, context)
      val receive = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("jobs"), ResourceKind.Queue, Capability.QueueReceive)), Nil), naming, context)
      val arn     = "arn:aws:sqs:us-east-1:123456789012:photo-app-prod-jobs"
      assertTrue(
        send.map(_.permissions.map(_._1)) == Right(Set("sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:SendMessage")),
        receive.map(_.permissions.map(_._1)).exists(a => a.contains("sqs:ReceiveMessage") && a.contains("sqs:DeleteMessage") && !a.contains("sqs:SendMessage")),
        send.map(_.permissions.map(_._2)) == Right(Set(arn)),
      )
    ,
    test("a secret reader may read that one secret"):
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("db"), ResourceKind.Secret, Capability.SecretRead)), Nil), naming, context)
      assertTrue(
        doc.map(_.permissions) == Right(
          Set(
            "secretsmanager:DescribeSecret" -> "arn:aws:secretsmanager:us-east-1:123456789012:secret:iae/photo-app/prod/db-??????",
            "secretsmanager:GetSecretValue" -> "arn:aws:secretsmanager:us-east-1:123456789012:secret:iae/photo-app/prod/db-??????",
          )
        )
      )
    ,
    test("table readers may only get items; writers may put and delete them"):
      val read  = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("photos"), ResourceKind.Table, Capability.TableRead)), Nil), naming, context)
      val write = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("photos"), ResourceKind.Table, Capability.TableWrite)), Nil), naming, context)
      val arn   = "arn:aws:dynamodb:us-east-1:123456789012:table/photo-app-prod-photos"
      assertTrue(
        read.map(_.permissions) == Right(Set("dynamodb:DescribeTable" -> arn, "dynamodb:GetItem" -> arn)),
        write.map(_.permissions) == Right(Set("dynamodb:DeleteItem" -> arn, "dynamodb:PutItem" -> arn)),
      )
    ,
    test("a topic publisher may publish to that one topic"):
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(Grant(LogicalId("events"), ResourceKind.Topic, Capability.TopicPublish)), Nil), naming, context)
      assertTrue(doc.map(_.permissions) == Right(Set("sns:Publish" -> "arn:aws:sns:us-east-1:123456789012:photo-app-prod-events")))
    ,
    test("subscribing a queue keeps the other statements in its policy and replaces only ours"):
      val theirs  = """{"Version":"2012-10-17","Statement":[{"Sid":"someone-else","Effect":"Allow","Principal":"*","Action":"sqs:GetQueueUrl","Resource":"q"}]}"""
      val ours    = TopicProvider.statement("arn:aws:sns:us-east-1:1:events", "arn:aws:sqs:us-east-1:1:q")
      val once    = TopicProvider.withStatement(Some(theirs), ours)
      val twice   = TopicProvider.withStatement(Some(once), ours)
      assertTrue(once.contains("someone-else"), once.contains("iae-events"), twice == once)
    ,
    test("documents round-trip through the JSON IAM stores"):
      val spec  = RoleSpec(List(grant("uploads", Capability.BucketRead), grant("thumbs", Capability.BucketWrite)), List(TrustedService.EcsTasks))
      val doc   = IamPolicy.permissions(app, env, spec, naming, context)
      val trust = IamPolicy.trust(spec)
      assertTrue(
        doc.map(d => IamPolicy.parsePermissions(d.toJson)) == doc.map(Some(_)),
        IamPolicy.parseTrust(trust.toJson).contains(trust),
        trust.services == Set("ecs-tasks.amazonaws.com"),
        trust.toJson.contains("\"Action\":[\"sts:AssumeRole\"]"),
      )
    ,
  )
