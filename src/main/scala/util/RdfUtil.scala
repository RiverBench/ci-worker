package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.VCARD

object RdfUtil:
  val m = ModelFactory.createDefaultModel()

  val pRb = "https://riverbench.github.io/schema/dataset#"

  val maximum = m.createProperty(pRb, "maximum")
  val minimum = m.createProperty(pRb, "minimum")
  val mean = m.createProperty(pRb, "mean")
  val stDev = m.createProperty(pRb, "standardDeviation")
  val sum = m.createProperty(pRb, "sum")
  val uniqueCount = m.createProperty(pRb, "uniqueCount")

  val hasStatistics = m.createProperty(pRb, "hasStatistics")
