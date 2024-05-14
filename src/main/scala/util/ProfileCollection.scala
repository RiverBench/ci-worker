package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr

import java.nio.file.Path
import scala.jdk.CollectionConverters.*

class ProfileCollection(profileDir: Path):
  val profiles = profileDir.toFile.listFiles().toSeq
    .filter(_.isFile)
    .filter(_.getName.endsWith(".ttl"))
    .filter(_.getName != "template.ttl")
    .map(f => (
      f.getName.replace(".ttl", "").replace("profile-", ""),
      RDFDataMgr.loadModel(f.toString)
    ))
    .toMap

  def getSubsets: Map[String, Seq[String]] =
    profiles.map { case (name, model) =>
      val subsets = model.listObjectsOfProperty(RdfUtil.isSupersetOfProfile).asScala
        .map(_.asResource().getLocalName)
        .toSeq
      (name, subsets)
    }

  def getSubSuperAssertions: Model =
    val m = ModelFactory.createDefaultModel()
    val subsetMap = getSubsets

    def addAssertions(superset: String, subsets: Seq[String], level: Int): Unit =
      if level > 10 then
        throw new Exception("Too many levels of nesting")

      val supRes = m.createResource(AppConfig.CiWorker.baseDevProfileUrl + superset)
      for sub <- subsets do
        val subRes = m.createResource(AppConfig.CiWorker.baseDevProfileUrl + sub)
        m.add(subRes, RdfUtil.isSubsetOfProfile, supRes)
        m.add(supRes, RdfUtil.isSupersetOfProfile, subRes)
        addAssertions(superset, subsetMap(sub), level + 1)

    subsetMap.foreach { case (name, subset) =>
      addAssertions(name, subset, 0)
    }

    m


