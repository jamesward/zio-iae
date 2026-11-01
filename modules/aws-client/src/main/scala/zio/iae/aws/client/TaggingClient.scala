package zio.iae.aws.client

import zio.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.fieldName
import zio.stream.ZStream

/** The Resource Groups Tagging API: finds resources of any kind by their tags. */
final case class TaggingClient(json: AwsJsonClient):
  import TaggingClient.*

  def resources(filters: Map[String, String]): ZStream[Any, AwsError, TaggedResource] =
    ZStream.paginateChunkZIO(Option.empty[String]): token =>
      val request = GetResourcesRequest(filters.toList.sorted.map((k, v) => TagFilter(k, List(v))), token)
      json
        .call[GetResourcesRequest, GetResourcesResponse](protocol, "GetResources", request)
        .map: response =>
          val page = Chunk.fromIterable(response.mappings.getOrElse(Nil)).map(m => TaggedResource(m.arn, m.tags.getOrElse(Nil).map(t => t.key -> t.value).toMap))
          (page, response.paginationToken.filter(_.nonEmpty).map(Some(_)))

final case class TaggedResource(arn: String, tags: Map[String, String])

object TaggingClient:
  val layer: ZLayer[AwsJsonClient, Nothing, TaggingClient] = ZLayer.fromFunction(TaggingClient.apply)

  private val protocol = AwsJsonProtocol(AwsService("tagging", "tagging"), "ResourceGroupsTaggingAPI_20170126", "1.1")

  private final case class TagFilter(@fieldName("Key") key: String, @fieldName("Values") values: List[String]) derives Schema
  private final case class GetResourcesRequest(
      @fieldName("TagFilters") tagFilters: List[TagFilter],
      @fieldName("PaginationToken") paginationToken: Option[String],
  ) derives Schema
  private final case class Tag(@fieldName("Key") key: String, @fieldName("Value") value: String) derives Schema
  private final case class Mapping(@fieldName("ResourceARN") arn: String, @fieldName("Tags") tags: Option[List[Tag]]) derives Schema
  private final case class GetResourcesResponse(
      @fieldName("ResourceTagMappingList") mappings: Option[List[Mapping]],
      @fieldName("PaginationToken") paginationToken: Option[String],
  ) derives Schema
