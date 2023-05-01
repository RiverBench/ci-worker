package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource}
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS, VCARD}

import scala.jdk.CollectionConverters.*

object RdfUtil:
  val m = ModelFactory.createDefaultModel()

  // Prefix for RiverBench
  val pRb = "https://w3id.org/riverbench/schema/metadata#"
  // Prefix for RiverBench documentation schema
  val pRbDoc = "https://w3id.org/riverbench/schema/documentation#"
  // Prefix for temporary resources
  val pTemp = "https://w3id.org/riverbench/temp/"
  // Prefix for DCAT
  val pDcat = "http://www.w3.org/ns/dcat#"
  // Prefix for DCTERMS
  val pDcterms = "http://purl.org/dc/terms/"
  // Prefix for FOAF
  val pFoaf = "http://xmlns.com/foaf/0.1/"
  // Prefix for SPDX
  val pSpdx = "http://spdx.org/rdf/terms#"

  // Properties
  val maximum = m.createProperty(pRb, "maximum")
  val minimum = m.createProperty(pRb, "minimum")
  val mean = m.createProperty(pRb, "mean")
  val stDev = m.createProperty(pRb, "standardDeviation")
  val sum = m.createProperty(pRb, "sum")
  val uniqueCount = m.createProperty(pRb, "uniqueCount")

  val hasStatistics = m.createProperty(pRb, "hasStatistics")
  val hasDistributionType = m.createProperty(pRb, "hasDistributionType")
  val hasStreamElementCount = m.createProperty(pRb, "hasStreamElementCount")
  val hasStreamElementType = m.createProperty(pRb, "hasStreamElementType")
  val hasFileName = m.createProperty(pRb, "hasFileName")
  val hasVersion = m.createProperty(pRb, "hasVersion")
  val hasRestriction = m.createProperty(pRb, "hasRestriction")
  val hasProfile = m.createProperty(pRb, "hasProfile")
  val hasTemporalProperty = m.createProperty(pRb, "hasTemporalProperty")
  val isSubsetOfProfile = m.createProperty(pRb, "isSubsetOfProfile")
  val isSupersetOfProfile = m.createProperty(pRb, "isSupersetOfProfile")

  val hasDocWeight = m.createProperty(pRbDoc, "hasDocWeight")
  val hasDocGroup = m.createProperty(pRbDoc, "hasDocGroup")
  val hasLabelOverride = m.createProperty(pRbDoc, "hasLabelOverride")
  val isHiddenInDoc = m.createProperty(pRbDoc, "isHiddenInDoc")

  val dcatDistribution = m.createProperty(pDcat, "distribution")
  val dcatByteSize = m.createProperty(pDcat, "byteSize")
  val dcatMediaType = m.createProperty(pDcat, "mediaType")
  val dcatCompressFormat = m.createProperty(pDcat, "compressFormat")
  val dcatPackageFormat = m.createProperty(pDcat, "packageFormat")
  val dcatDownloadURL = m.createProperty(pDcat, "downloadURL")
  val dcatLandingPage = m.createProperty(pDcat, "landingPage")
  val dcatInCatalog = m.createProperty(pDcat, "inCatalog")
  val dcatInSeries = m.createProperty(pDcat, "inSeries")
  val dcatSeriesMember = m.createProperty(pDcat, "seriesMember")
  val dcatDataset = m.createProperty(pDcat, "dataset")

  val dctermsDescription = m.createProperty(pDcterms, "description")
  val dctermsIdentifier = m.createProperty(pDcterms, "identifier")
  val dctermsTitle = m.createProperty(pDcterms, "title")
  val dctermsModified = m.createProperty(pDcterms, "modified")

  val foafHomepage = m.createProperty(pFoaf, "homepage")

  val spdxChecksum = m.createProperty(pSpdx, "checksum")
  val spdxAlgorithm = m.createProperty(pSpdx, "algorithm")
  val spdxChecksumValue = m.createProperty(pSpdx, "checksumValue")

  // Classes
  val Dataset = m.createResource(pRb + "Dataset")
  val Distribution = m.createResource(pRb + "Distribution")
  val Profile = m.createResource(pRb + "Profile")
  val RiverBench = m.createResource(pRb + "RiverBench")
  val DocGroup = m.createResource(pRbDoc + "DocGroup")
  val DcatDistribution = m.createResource(pDcat + "Distribution")
  val SpdxChecksum = m.createResource(pSpdx + "Checksum")

  // Instances
  val partialDistribution = m.createResource(pRb + "partialDistribution")
  val fullDistribution = m.createResource(pRb + "fullDistribution")
  val tripleStreamDistribution = m.createResource(pRb + "tripleStreamDistribution")
  val quadStreamDistribution = m.createResource(pRb + "quadStreamDistribution")
  val graphStreamDistribution = m.createResource(pRb + "graphStreamDistribution")
  val flatDistribution = m.createResource(pRb + "flatDistribution")

  val spdxChecksumAlgorithmSha1 = m.createResource(pSpdx + "checksumAlgorithm_sha1")
  val spdxChecksumAlgorithmMd5 = m.createResource(pSpdx + "checksumAlgorithm_md5")

  val tempDataset = m.createResource(pTemp + "dataset")

  def getString(subject: Resource, prop: Property, m: Option[Model] = None): Option[String] =
    val model = m.getOrElse(subject.getModel)
    val langString = model.getProperty(subject, prop, "en")
    if langString != null then
      Some(langString.getString)
    else
      val string = model.getProperty(subject, prop)
      if string != null then Some(string.getString) else None

  def getLabel(subject: Resource, m: Option[Model] = None): String =
    (
      getString(subject, RdfUtil.hasLabelOverride, m) ++
      getString(subject, SKOS.prefLabel, m) ++
      getString(subject, RDFS.label, m)
    ).headOption.getOrElse(subject.getLocalName)

  def getPrettyLabel(subject: Resource, m: Option[Model] = None): String =
    getLabel(subject, m).strip.capitalize

  /**
   * Checks if a resource is probably a named thing.
   * @return
   */
  def isNamedThing(subject: Resource, m: Option[Model]): Boolean =
    val model = m.getOrElse(subject.getModel)
    model.getProperty(subject, RDF.`type`) != null

  def renameResource(oldResource: Resource, newResource: Resource, model: Model): Unit =
    if oldResource.getURI == newResource.getURI then return
    val newStatements = for stmt <- model.listStatements.asScala yield
      if stmt.getSubject == oldResource then
        Some(newResource, stmt.getPredicate, stmt.getObject)
      else if stmt.getObject == oldResource then
        Some(stmt.getSubject, stmt.getPredicate, newResource)
      else None

    for stmt <- newStatements.flatten.toSeq do
      model.add(stmt._1, stmt._2, stmt._3)

    model.removeAll(oldResource, null, null)
    model.removeAll(null, null, oldResource)
