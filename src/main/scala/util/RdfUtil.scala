package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.{AnonId, Model, ModelFactory, Property, Resource}
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS, VCARD}

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Random

object RdfUtil:
  val m = ModelFactory.createDefaultModel()

  // Prefix for RiverBench
  val pRb = "https://w3id.org/riverbench/schema/metadata#"
  // Prefix for RiverBench documentation schema
  val pRbDoc = "https://w3id.org/riverbench/schema/documentation#"
  // Prefix for temporary resources
  val pTemp = "https://w3id.org/riverbench/temp#"
  // Prefix for SHACL libs
  val pLib = "https://w3id.org/riverbench/temp/lib#"
  // Prefix for DCAT
  val pDcat = "http://www.w3.org/ns/dcat#"
  // Prefix for DCTERMS
  val pDcterms = "http://purl.org/dc/terms/"
  // Prefix for FOAF
  val pFoaf = "http://xmlns.com/foaf/0.1/"
  // Prefix for IRAO
  val pIrao = "http://ontology.ethereal.cz/irao/"
  // Prefix for NP (nanopubs)
  val pNp = "http://www.nanopub.org/nschema#"
  // Prefix for OWL
  val pOwl = "http://www.w3.org/2002/07/owl#"
  // Prefix for RDFS
  val pRdfs = "http://www.w3.org/2000/01/rdf-schema#"
  // Prefix for SHACL
  val pSh = "http://www.w3.org/ns/shacl#"
  // Prefix for SPDX
  val pSpdx = "http://spdx.org/rdf/terms#"
  // Prefix for RDF-STaX
  val pStax = "https://w3id.org/stax/ontology#"

  // Properties
  val maximum = m.createProperty(pRb, "maximum")
  val minimum = m.createProperty(pRb, "minimum")
  val mean = m.createProperty(pRb, "mean")
  val stDev = m.createProperty(pRb, "standardDeviation")
  val sum = m.createProperty(pRb, "sum")
  val uniqueCount = m.createProperty(pRb, "uniqueCount")

  val hasStatistics = m.createProperty(pRb, "hasStatistics")
  val hasStatisticsSet = m.createProperty(pRb, "hasStatisticsSet")
  val hasDistributionType = m.createProperty(pRb, "hasDistributionType")
  val hasStreamElementCount = m.createProperty(pRb, "hasStreamElementCount")
  val hasFileName = m.createProperty(pRb, "hasFileName")
  val hasVersion = m.createProperty(pRb, "hasVersion")
  val hasRestriction = m.createProperty(pRb, "hasRestriction")
  val hasTemporalProperty = m.createProperty(pRb, "hasTemporalProperty")
  val hasSubjectShape = m.createProperty(pRb, "hasSubjectShape")
  val isSubsetOfProfile = m.createProperty(pRb, "isSubsetOfProfile")
  val isSupersetOfProfile = m.createProperty(pRb, "isSupersetOfProfile")
  val targetCustom = m.createProperty(pRb, "targetCustom")
  val hasDatasetShape = m.createProperty(pRb, "hasDatasetShape")
  val hasDistributionShape = m.createProperty(pRb, "hasDistributionShape")
  val inCategory = m.createProperty(pRb, "inCategory")
  val hasTask = m.createProperty(pRb, "hasTask")
  val hasProfile = m.createProperty(pRb, "hasProfile")
  val hasCategory = m.createProperty(pRb, "hasCategory")
  val usesTask = m.createProperty(pRb, "usesTask")
  val usesProfile = m.createProperty(pRb, "usesProfile")

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
  val dcatResource = m.createProperty(pDcat, "resource")

  val dctermsCreated = m.createProperty(pDcterms, "created")
  val dctermsCreator = m.createProperty(pDcterms, "creator")
  val dctermsDescription = m.createProperty(pDcterms, "description")
  val dctermsIdentifier = m.createProperty(pDcterms, "identifier")
  val dctermsLicense = m.createProperty(pDcterms, "license")
  val dctermsTitle = m.createProperty(pDcterms, "title")
  val dctermsModified = m.createProperty(pDcterms, "modified")

  val foafHomepage = m.createProperty(pFoaf, "homepage")

  val iraoHasFollowedProtocol = m.createProperty(pIrao, "hasFollowedProtocol")
  
  val npHasAssertion = m.createProperty(pNp, "hasAssertion")
  val npHasPublicationInfo = m.createProperty(pNp, "hasPublicationInfo")

  val owlVersionIri = m.createProperty(pOwl, "versionIRI")

  val shTargetClass = m.createProperty(pSh, "targetClass")
  val shTargetSubjectsOf = m.createProperty(pSh, "targetSubjectsOf")
  val shTargetObjectsOf = m.createProperty(pSh, "targetObjectsOf")

  val spdxChecksum = m.createProperty(pSpdx, "checksum")
  val spdxAlgorithm = m.createProperty(pSpdx, "algorithm")
  val spdxChecksumValue = m.createProperty(pSpdx, "checksumValue")

  val staxHasStreamType = m.createProperty(pStax, "hasStreamType")
  val staxHasStreamTypeUsage = m.createProperty(pStax, "hasStreamTypeUsage")

  // Classes
  val Dataset = m.createResource(pRb + "Dataset")
  val Distribution = m.createResource(pRb + "Distribution")
  val Profile = m.createResource(pRb + "Profile")
  val RiverBench = m.createResource(pRb + "RiverBench")
  val StatisticsSet = m.createResource(pRb + "StatisticsSet")
  val TopicStreamElementSplit = m.createResource(pRb + "TopicStreamElementSplit")
  val Category = m.createResource(pRb + "Category")
  val Task = m.createResource(pRb + "Task")
  val StatementCountStatistics = m.createResource(pRb + "StatementCountStatistics")
  val SystemUnderTest = m.createResource(pRb + "SystemUnderTest")
  val DocGroup = m.createResource(pRbDoc + "DocGroup")

  val DcatDistribution = m.createResource(pDcat + "Distribution")
  val SpdxChecksum = m.createResource(pSpdx + "Checksum")

  // Instances
  val partialDistribution = m.createResource(pRb + "partialDistribution")
  val fullDistribution = m.createResource(pRb + "fullDistribution")
  val streamDistribution = m.createResource(pRb + "streamDistribution")
  val flatDistribution = m.createResource(pRb + "flatDistribution")
  val jellyDistribution = m.createResource(pRb + "jellyDistribution")
  val yagoTarget = m.createResource(pRb + "yagoTarget")

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
    
  def removePropertyValuesDeep(subject: Resource, property: Property, model: Model): Unit =
    val statements = model.listStatements(subject, property, null)
    for stmt <- statements.asScala do
      model.removeAll(stmt.getObject.asResource, null, null)
    model.removeAll(subject, property, null)

  def mergeModels(models: IterableOnce[Model]): Model =
    val model = ModelFactory.createDefaultModel()
    for m <- models.iterator do
      model.add(m)
    model

  
  def newAnonId(seed: Object): AnonId =
    val r = Random(seed.hashCode())
    AnonId.create(UUID(r.nextLong(), r.nextLong()).toString)

  /**
   * Creates a new anonymous ID for a blank node based on some bytes as the seed.
   * Note that for two identical seeds, the same ID will be generated.
   *
   * @param seed any hashable object
   * @return a new AnonId
   */
  def newAnonId(seed: Array[Byte]): AnonId =
    AnonId.create(UUID.nameUUIDFromBytes(seed).toString)