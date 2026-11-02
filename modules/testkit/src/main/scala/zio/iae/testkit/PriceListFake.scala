package zio.iae.testkit

import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/** The Price List API's `GetProducts` over products the test adds, as Floci doesn't emulate it. Filters on attributes
  * are applied as AWS does (`TERM_MATCH` equal, `CONTAINS` substring). Point an `AwsConfig`'s endpoint at `url`.
  */
final case class PriceListFake(url: URL, products: Ref[List[(String, Json.Obj)]]):
  /** A product of the service in a region, priced per unit, after a free tier of `free` units when given. */
  def add(service: String, region: String, usageType: String, usd: Double, family: String = "", free: Option[Double] = None): UIO[Unit] =
    val tiers = free.fold(List(("0", usd))): f =>
      List(("0", 0d), (f.toLong.toString, usd))
    val dimensions = tiers.zipWithIndex.map: (tier, i) =>
      s"RATE$i" -> Json.Obj("beginRange" -> Json.Str(tier._1), "pricePerUnit" -> Json.Obj("USD" -> Json.Str(tier._2.toString)))
    val product = Json.Obj(
      "product" -> Json.Obj(
        "productFamily" -> Json.Str(family),
        "attributes"    -> Json.Obj("regionCode" -> Json.Str(region), "usagetype" -> Json.Str(usageType)),
      ),
      "terms" -> Json.Obj("OnDemand" -> Json.Obj("TERM1" -> Json.Obj("priceDimensions" -> Json.Obj(dimensions*)))),
    )
    products.update(_ :+ (service -> product))

object PriceListFake:
  val make: ZIO[Scope, Throwable, PriceListFake] =
    for
      products <- Ref.make(List.empty[(String, Json.Obj)])
      server   <- Server.defaultWithPort(0).build
      port     <- Server.install(routes(products)).provideEnvironment(server)
      url      <- ZIO.fromEither(URL.decode(s"http://localhost:$port"))
    yield PriceListFake(url, products)

  private def matches(product: Json.Obj, filter: Json): Boolean =
    val f     = filter.asObject
    val field = f.flatMap(_.get("Field")).flatMap(_.asString).getOrElse("")
    val value = f.flatMap(_.get("Value")).flatMap(_.asString).getOrElse("")
    val tpe   = f.flatMap(_.get("Type")).flatMap(_.asString).getOrElse("")
    val p     = product.get("product").flatMap(_.asObject)
    val attr  = if field == "productFamily" then p.flatMap(_.get("productFamily")) else p.flatMap(_.get("attributes")).flatMap(_.asObject).flatMap(_.get(field))
    attr.flatMap(_.asString).exists(a => if tpe == "CONTAINS" then a.contains(value) else a == value)

  private def routes(products: Ref[List[(String, Json.Obj)]]): Routes[Any, Nothing] =
    Routes(
      Method.POST / trailing -> handler: (req: Request) =>
        for
          body    <- req.body.asString.orElseSucceed("")
          json     = body.fromJson[Json.Obj].getOrElse(Json.Obj())
          service  = json.get("ServiceCode").flatMap(_.asString).getOrElse("")
          filters  = json.get("Filters").flatMap(_.asArray).getOrElse(Chunk.empty)
          all     <- products.get
          found    = all.collect { case (s, p) if s == service && filters.forall(matches(p, _)) => Json.Str(p.toJson) }
        yield Response.json(Json.Obj("PriceList" -> Json.Arr(found*)).toJson),
    )
