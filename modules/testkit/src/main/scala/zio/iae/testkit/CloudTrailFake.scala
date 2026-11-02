package zio.iae.testkit

import java.time.Instant
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** CloudTrail's `LookupEvents` with events the test records, since Floci records management events only for Secrets
  * Manager. Point an `AwsConfig`'s endpoint at `url`.
  */
final case class CloudTrailFake(url: URL, events: Ref[List[CloudTrailFake.Recorded]]):
  /** Records that `principal` called `name` on the resource at `at`. */
  def record(resource: String, name: String, principal: String, at: Instant, readOnly: Boolean = false): UIO[Unit] =
    events.update(_ :+ CloudTrailFake.Recorded(resource, name, principal, at, readOnly))

object CloudTrailFake:
  final case class Recorded(resource: String, name: String, principal: String, at: Instant, readOnly: Boolean)

  val make: ZIO[Scope, Throwable, CloudTrailFake] =
    for
      events <- Ref.make(List.empty[Recorded])
      server <- Server.defaultWithPort(0).build
      port   <- Server.install(routes(events)).provideEnvironment(server)
      url    <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield CloudTrailFake(url, events)

  private def routes(events: Ref[List[Recorded]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / trailing -> handler: (req: Request) =>
        for
          body <- req.body.asString.orElseSucceed("")
          obj   = body.fromJson[Json.Obj].toOption
          name  = obj.flatMap(_.get("LookupAttributes")).flatMap(_.asArray).flatMap(_.headOption).flatMap(_.asObject)
                    .flatMap(_.get("AttributeValue")).flatMap(_.asString)
          since = obj.flatMap(_.get("StartTime")).flatMap(_.asNumber).map(_.value.longValue).getOrElse(0L)
          all  <- events.get
          found = all.filter(e => name.contains(e.resource) && e.at.getEpochSecond >= since).sortBy(_.at).reverse
        yield Response.json(
          Json.Obj("Events" -> Json.Arr(Chunk.fromIterable(found).map: e =>
            Json.Obj(
              "EventName"       -> Json.Str(e.name),
              "EventTime"       -> Json.Num(e.at.getEpochSecond.toDouble),
              "Username"        -> Json.Str(e.principal.split('/').last),
              "ReadOnly"        -> Json.Str(e.readOnly.toString),
              "CloudTrailEvent" -> Json.Str(Json.Obj("userIdentity" -> Json.Obj("arn" -> Json.Str(e.principal)), "readOnly" -> Json.Bool(e.readOnly)).toJson),
            )
          *)).toJson
        ),
    )
