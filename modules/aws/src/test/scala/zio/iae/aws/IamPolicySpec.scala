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
