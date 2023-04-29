package io.github.riverbench.ci_worker
package util

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
  conformance: ConformanceInfo = ConformanceInfo(),
  datasetRes: Resource = null,
)

case class ConformanceInfo(conformsToRdf11: Boolean = false, conformsToRdfStarDraft_20211217: Boolean = false,
                           usesGeneralizedRdfDatasets: Boolean = false, usesGeneralizedTriples: Boolean = false,
                           usesRdfStar: Boolean = false):
  def checkConsistency(): Seq[String] =
    val errors = Seq.newBuilder[String]
    if conformsToRdf11 && (usesGeneralizedRdfDatasets || usesGeneralizedTriples || usesRdfStar) then
      errors += "Dataset conforms to RDF1.1, but also uses a non-standard feature"
    if !conformsToRdf11 && conformsToRdfStarDraft_20211217 && !usesRdfStar then
      errors += "Dataset does not use RDF-star and does not conform to RDF1.1, " +
        "but conforms to RDF-star draft 2021-12-17"
    if conformsToRdf11 && !conformsToRdfStarDraft_20211217 then
      errors += "Dataset conforms to RDF1.1, but not to RDF-star draft 2021-12-17, which is a superset of RDF1.1"
    errors.result()

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
      conformance = ConformanceInfo(
        conformsToRdf11 = getBool("conformsToRdf11"),
        conformsToRdfStarDraft_20211217 = getBool("conformsToRdfStarDraft_20211217"),
        usesGeneralizedRdfDatasets = getBool("usesGeneralizedRdfDatasets"),
        usesGeneralizedTriples = getBool("usesGeneralizedTriples"),
        usesRdfStar = getBool("usesRdfStar"),
      ),
      datasetRes = dataset,
    )
