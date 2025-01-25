package io.github.riverbench.ci_worker
package util.rdf

import commands.PackageCommand.DistType
import util.*

import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.RDF

import java.nio.file.Path
import scala.jdk.CollectionConverters.*


case class MetadataInfo(
  identifier: String = "",
  description: String = "",
  streamTypes: Set[StreamType] = Set.empty,
  elementCount: Long = 0,
  temporalProp: Option[Resource] = None,
  subjectNodeShapes: Seq[SubjectNodeShape] = Seq(),
  conformance: ConformanceInfo = ConformanceInfo(),
  datasetRes: Resource = null,
  model: Model = null,
):
  def checkConsistency(): Seq[String] =
    val c = conformance
    val errors = Seq.newBuilder[String]
    if c.conformsToRdf11 && (c.usesGeneralizedRdfDatasets || c.usesGeneralizedTriples || c.usesRdfStar) then
      errors += "Dataset conforms to RDF1.1, but also uses a non-standard feature"
    if !c.conformsToRdf11 && c.conformsToRdfStarDraft_20211217 && !c.usesRdfStar then
      errors += "Dataset does not use RDF-star and does not conform to RDF1.1, " +
        "but conforms to RDF-star draft 2021-12-17"
    if c.conformsToRdf11 && !c.conformsToRdfStarDraft_20211217 then
      errors += "Dataset conforms to RDF1.1, but not to RDF-star draft 2021-12-17, which is a superset of RDF1.1"
    if c.usesGeneralizedRdfDatasets && streamTypes.exists(_.elementType == ElementType.Triple) then
      errors += "Dataset uses generalized RDF datasets, but is a triple stream"
    if elementCount <= 0 then
      errors += "Dataset has an invalid element count"
    if temporalProp.isEmpty && streamTypes.contains(StreamType.TimestampedNamedGraph) then
      errors += "Dataset has no temporal property, but it is a timestamped RDF named graph stream."
    if subjectNodeShapes.isEmpty && streamTypes.contains(StreamType.SubjectGraph) then
      errors += "Dataset has no subject node shape specified, but it is a subject graph stream."
    errors.result()

  def addStreamTypesToRdf(datasetRes: Resource): Unit =
    for sType <- streamTypes do
      val sTypeIri = model.createResource(sType.iri)
      val sTypeUsage = model.listResourcesWithProperty(RdfUtil.staxHasStreamType, sTypeIri)
        .asScala.toSeq.head
      val newUsage = datasetRes.getModel.createResource(RdfUtil.newAnonId(sType.iri.getBytes))
      sTypeUsage.listProperties().asScala
        .foreach(p => newUsage.addProperty(p.getPredicate, p.getObject))
      datasetRes.addProperty(RdfUtil.staxHasStreamTypeUsage, newUsage)

  def addToRdf(distRes: Resource, size: Long, dType: DistType): Resource =
    // Info about the distribution
    // Media type, byte size, link, checksum are added separately by the packaging pipeline
    distRes.addProperty(RDF.`type`, RdfUtil.Distribution)
    distRes.addProperty(RDF.`type`, RdfUtil.DcatDistribution)

    val weight = dType.weight + (
      if size == elementCount then
        0
      else
        (elementCount.toString.length - size.toString.length + 1) * 3
      )
    distRes.addLiteral(RdfUtil.hasDocWeight, weight)

    val sizeString = if size == elementCount then
      distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.fullDistribution)
      "Full"
    else
      distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.partialDistribution)
      Constants.packageSizeToHuman(size) + " elements"

    // Assigns the stream type usage from the dataset to the distribution
    def addStreamTypes(types: Iterable[StreamType]): Unit =
      val m = distRes.getModel
      for sType <- types do
        val sTypeIri = m.createResource(sType.iri)
        val sTypeUsage = m.listResourcesWithProperty(RdfUtil.staxHasStreamType, sTypeIri)
          .asScala.toSeq.head
        distRes.addProperty(RdfUtil.staxHasStreamTypeUsage, sTypeUsage)

    dType match
      case DistType.Flat =>
        addStreamTypes(streamTypes.filter(_.isFlat))
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.flatDistribution)
        distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString flat distribution")
      case DistType.Stream =>
        addStreamTypes(streamTypes.filterNot(_.isFlat))
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.streamDistribution)
        distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString stream distribution")
      case DistType.Jelly =>
        addStreamTypes(streamTypes)
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.jellyDistribution)
        distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString Jelly distribution")

    distRes.addLiteral(RdfUtil.hasStreamElementCount, size)

case class ConformanceInfo(conformsToRdf11: Boolean = false, conformsToRdfStarDraft_20211217: Boolean = false,
                           usesGeneralizedRdfDatasets: Boolean = false, usesGeneralizedTriples: Boolean = false,
                           usesRdfStar: Boolean = false)

object MetadataReader:
  /**
   * Read metadata from a dataset repository.
   * Assumes that the metadata file is valid.
   * @param repoDir Path to the dataset repository
   * @return
   */
  def read(repoDir: Path): MetadataInfo =
    val model = RdfIoUtil.loadModelWithStableBNodeIds(repoDir.resolve("metadata.ttl"))
    fromModel(model)

  def fromModel(model: Model): MetadataInfo =
    def getBool(prop: String): Boolean =
      model.listObjectsOfProperty(model.createProperty(RdfUtil.pRb + prop))
        .asScala.toSeq.head.asLiteral.getBoolean

    val dataset = model.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    val types = dataset.listProperties(RdfUtil.staxHasStreamTypeUsage).asScala
      .map(st => st.getResource.listProperties(RdfUtil.staxHasStreamType).asScala.toSeq.head)
      .map(st => st.getResource.getLocalName)
      .map(name => StreamType.values.find(_.iriName == name).get)
      .toSet

    MetadataInfo(
      identifier = dataset.listProperties(RdfUtil.dctermsIdentifier)
        .asScala.toSeq.head.getLiteral.getString.strip,
      description = dataset.listProperties(RdfUtil.dctermsDescription)
        .asScala.toSeq.head.getLiteral.getString.strip,
      streamTypes = types,
      elementCount = dataset.listProperties(RdfUtil.hasStreamElementCount)
        .asScala.toSeq.head.getLiteral.getLong,
      temporalProp = model.listObjectsOfProperty(RdfUtil.hasTemporalProperty)
        .asScala.toSeq.headOption.map(_.asResource),
      subjectNodeShapes = model.listResourcesWithProperty(RDF.`type`, RdfUtil.TopicStreamElementSplit)
        .asScala.toSeq.headOption.map(SubjectNodeShape.fromElementSplit).getOrElse(Seq()),
      conformance = ConformanceInfo(
        conformsToRdf11 = getBool("conformsToRdf11"),
        conformsToRdfStarDraft_20211217 = getBool("conformsToRdfStarDraft_20211217"),
        usesGeneralizedRdfDatasets = getBool("usesGeneralizedRdfDatasets"),
        usesGeneralizedTriples = getBool("usesGeneralizedTriples"),
        usesRdfStar = getBool("usesRdfStar"),
      ),
      datasetRes = dataset,
      model = model,
    )
