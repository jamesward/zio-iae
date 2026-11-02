package zio.iae.aws.client

import java.time.Instant
import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.schema.{Schema, derived}

/** A management event CloudTrail recorded: who did what, when. `principal` is the caller's ARN (an assumed role's
  * session, a user), from the event's `userIdentity`.
  */
final case class TrailEvent(name: String, at: Instant, principal: Option[String], username: Option[String], readOnly: Boolean)

/** CloudTrail's event history: what drift attribution reads. */
final case class CloudTrailClient(json: AwsJsonClient):
  import CloudTrailClient.*

  /** The events on a resource (by name) since a time, newest first. */
  def lookup(resourceName: String, since: Instant): IO[AwsError, List[TrailEvent]] =
    json
      .call[Lookup, Found](protocol, "LookupEvents", Lookup(List(Attribute("ResourceName", resourceName)), since.getEpochSecond.toDouble, 50))
      .map(_.Events.getOrElse(Nil).map(event))

object CloudTrailClient:
  val layer: ZLayer[AwsJsonClient, Nothing, CloudTrailClient] = ZLayer.fromFunction(CloudTrailClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("cloudtrail", "cloudtrail"), "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101", "1.1")

  final case class Attribute(AttributeKey: String, AttributeValue: String) derives Schema
  final case class Lookup(LookupAttributes: List[Attribute], StartTime: Double, MaxResults: Int) derives Schema
  final case class Event(EventName: Option[String], EventTime: Option[Double], Username: Option[String], CloudTrailEvent: Option[String], ReadOnly: Option[String])
      derives Schema
  final case class Found(Events: Option[List[Event]]) derives Schema

  private def event(e: Event): TrailEvent =
    val detail = e.CloudTrailEvent.flatMap(_.fromJson[Json.Obj].toOption)
    val arn    = detail.flatMap(_.get("userIdentity")).flatMap(_.asObject).flatMap(_.get("arn")).flatMap(_.asString)
    val ro     = e.ReadOnly.map(_ == "true").orElse(detail.flatMap(_.get("readOnly")).flatMap(_.asBoolean)).getOrElse(false)
    TrailEvent(e.EventName.getOrElse("?"), Instant.ofEpochMilli((e.EventTime.getOrElse(0d) * 1000).toLong).nn, arn, e.Username, ro)
