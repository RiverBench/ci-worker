package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.VCARD

object RdfUtil:
  val m = ModelFactory.createDefaultModel()

  // Prefix for RiverBench
  val pRb = "https://riverbench.github.io/schema/dataset#"
  // Prefix for temporary resources
  val pTemp = "https://riverbench.github.io/temp#"
  // Prefix for DCAT
  val pDcat = "http://www.w3.org/ns/dcat#"
  // Prefix for DCTERMS
  val pDcterms = "http://purl.org/dc/terms/"
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
  val hasFileName = m.createProperty(pRb, "hasFileName")
  val hasVersion = m.createProperty(pRb, "hasVersion")

  val dcatDistribution = m.createProperty(pDcat, "distribution")
  val dcatByteSize = m.createProperty(pDcat, "byteSize")
  val dcatMediaType = m.createProperty(pDcat, "mediaType")
  val dcatCompressFormat = m.createProperty(pDcat, "compressFormat")
  val dcatPackageFormat = m.createProperty(pDcat, "packageFormat")
  val dcatDownloadURL = m.createProperty(pDcat, "downloadURL")
  val dcatLandingPage = m.createProperty(pDcat, "landingPage")

  val dctermsIdentifier = m.createProperty(pDcterms, "identifier")
  val dctermsTitle = m.createProperty(pDcterms, "title")

  val spdxChecksum = m.createProperty(pSpdx, "checksum")
  val spdxAlgorithm = m.createProperty(pSpdx, "algorithm")
  val spdxChecksumValue = m.createProperty(pSpdx, "checksumValue")

  // Classes
  val Dataset = m.createResource(pRb + "Dataset")
  val Distribution = m.createResource(pRb + "Distribution")
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
