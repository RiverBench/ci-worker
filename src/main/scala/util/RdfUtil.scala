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

  val dcatDistribution = m.createProperty(pDcat, "distribution")
  val dcatTitle = m.createProperty(pDcat, "title")

  // Classes
  val Distribution = m.createResource(pRb + "Distribution")
  val DcatDistribution = m.createResource(pDcat + "Distribution")

  // Instances
  val partialDistribution = m.createResource(pRb + "partialDistribution")
  val fullDistribution = m.createResource(pRb + "fullDistribution")
  val tripleStreamDistribution = m.createResource(pRb + "tripleStreamDistribution")
  val quadStreamDistribution = m.createResource(pRb + "quadStreamDistribution")
  val graphStreamDistribution = m.createResource(pRb + "graphStreamDistribution")
  val flatDistribution = m.createResource(pRb + "flatDistribution")
