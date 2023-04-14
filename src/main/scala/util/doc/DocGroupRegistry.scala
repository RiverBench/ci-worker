package io.github.riverbench.ci_worker
package util.doc

import io.github.riverbench.ci_worker.util.RdfUtil
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.jdk.CollectionConverters.*

class DocGroupRegistry(ontologies: Model):
  val docGroups = ontologies.listResourcesWithProperty(RDF.`type`, RdfUtil.DocGroup).asScala
    .map(dg => (dg.getURI, dg.getProperty(RDFS.label)))
    .toSeq

  private val map = docGroups.toMap

  def getDocGroup(uri: String): Option[String] =
    map.get(uri).map(_.getString)
