package zio.iae.aws

import zio.iae.*
import zio.iae.aws.client.BucketName

/** Physical S3 bucket names: `<app>-<env>-<id>`, plus a suffix (for example derived from the account) where global
  * uniqueness needs one.
  */
final case class BucketNaming(suffix: Option[String]):
  def physical(address: Address): Either[InfraError, BucketName] =
    val parts = List(address.app.value, address.env.value, address.id.value) ++ suffix.toList
    BucketName.from(parts.mkString("-")).left.map(InfraError.InvalidConfig(_))
