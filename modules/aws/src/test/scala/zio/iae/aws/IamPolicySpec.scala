package zio.iae.aws

import zio.iae.*
import zio.test.*

object IamPolicySpec extends ZIOSpecDefault:
  val app    = AppName("photo-app")
  val env    = EnvName("prod")
  val naming = BucketNaming(None)

  def grant(id: String, capability: Capability) = Grant(LogicalId.from(id).toOption.get, ResourceKind.Bucket, capability)

  def spec = suite("IamPolicy")(
    test("a reader may get and list one bucket, nothing else"):
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(grant("uploads", Capability.BucketRead)), Nil), naming)
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
      val doc = IamPolicy.permissions(app, env, RoleSpec(List(grant("uploads", Capability.BucketWrite)), Nil), naming)
      assertTrue(
        doc.map(_.permissions) == Right(
          Set("s3:DeleteObject" -> "arn:aws:s3:::photo-app-prod-uploads/*", "s3:PutObject" -> "arn:aws:s3:::photo-app-prod-uploads/*")
        ),
        doc.map(_.statements.map(_.sid)) == Right(List("BucketWriteUploads")),
      )
    ,
    test("documents round-trip through the JSON IAM stores"):
      val spec  = RoleSpec(List(grant("uploads", Capability.BucketRead), grant("thumbs", Capability.BucketWrite)), List(TrustedService.EcsTasks))
      val doc   = IamPolicy.permissions(app, env, spec, naming)
      val trust = IamPolicy.trust(spec)
      assertTrue(
        doc.map(d => IamPolicy.parsePermissions(d.toJson)) == doc.map(Some(_)),
        IamPolicy.parseTrust(trust.toJson).contains(trust),
        trust.services == Set("ecs-tasks.amazonaws.com"),
        trust.toJson.contains("\"Action\":[\"sts:AssumeRole\"]"),
      )
    ,
  )
