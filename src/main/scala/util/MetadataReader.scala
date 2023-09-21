package io.github.riverbench.ci_worker
package util

import commands.PackageCommand.DistType

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.RDF

import java.nio.file.Path
import scala.jdk.CollectionConverters.*

case class MetadataInfo(
  identifier: String = "",
  description: String = "",
  elementType: String = "",
  elementCount: Long = 0,
  temporalProp: Option[Resource] = None,
  conformance: ConformanceInfo = ConformanceInfo(),
  datasetRes: Resource = null,
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
    if c.usesGeneralizedRdfDatasets && elementType == "triples" then
      errors += "Dataset uses generalized RDF datasets, but has element type 'triples'"
    if elementCount <= 0 then
      errors += "Dataset has an invalid element count"
    if temporalProp.isEmpty && elementType == "graphs" then
      println("Warning: Dataset has no temporal property, but element type is 'graphs'. " +
        "Assuming it's a non-temporal dataset.")
    errors.result()

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
    distRes.addProperty(RdfUtil.hasDocWeight, weight.toString, XSDinteger)

    val sizeString = if size == elementCount then
      distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.fullDistribution)
      "Full"
    else
      distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.partialDistribution)
      Constants.packageSizeToHuman(size) + " elements"

    def addStreamType(t: String): Unit =
      elementType match
        case "graphs" =>
          distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.graphStreamDistribution)
          distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString graph stream distribution" + t)
        case "triples" =>
          distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.tripleStreamDistribution)
          distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString triple stream distribution" + t)
        case "quads" =>
          distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.quadStreamDistribution)
          distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString quad stream distribution" + t)

    dType match
      case DistType.Flat =>
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.flatDistribution)
        distRes.addProperty(RdfUtil.dctermsTitle, s"$sizeString flat distribution")
      case DistType.Stream =>
        addStreamType("")
      case DistType.Jelly =>
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.jellyDistribution)
        addStreamType(" (Jelly)")

    distRes.addProperty(RdfUtil.hasStreamElementCount, size.toString, XSDinteger)

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
    val model = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    fromModel(model)

  def fromModel(model: Model): MetadataInfo =
    def getBool(prop: String): Boolean =
      model.listObjectsOfProperty(model.createProperty(RdfUtil.pRb + prop))
        .asScala.toSeq.head.asLiteral.getBoolean

    val dataset = model.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    val types = dataset.listProperties(RdfUtil.hasStreamElementType)
      .asScala.toSeq

    MetadataInfo(
      identifier = dataset.listProperties(RdfUtil.dctermsIdentifier)
        .asScala.toSeq.head.getLiteral.getString.strip,
      description = dataset.listProperties(RdfUtil.dctermsDescription)
        .asScala.toSeq.head.getLiteral.getString.strip,
      elementType = types.head.getResource.getURI.split('#').last,
      elementCount = dataset.listProperties(RdfUtil.hasStreamElementCount)
        .asScala.toSeq.head.getLiteral.getLong,
      temporalProp = model.listObjectsOfProperty(RdfUtil.hasTemporalProperty)
        .asScala.toSeq.headOption.map(_.asResource),
      conformance = ConformanceInfo(
        conformsToRdf11 = getBool("conformsToRdf11"),
        conformsToRdfStarDraft_20211217 = getBool("conformsToRdfStarDraft_20211217"),
        usesGeneralizedRdfDatasets = getBool("usesGeneralizedRdfDatasets"),
        usesGeneralizedTriples = getBool("usesGeneralizedTriples"),
        usesRdfStar = getBool("usesRdfStar"),
      ),
      datasetRes = dataset,
    )
