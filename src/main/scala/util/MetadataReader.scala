package io.github.riverbench.ci_worker
package util

import org.apache.jena.riot.RDFDataMgr

import java.nio.file.Path
import scala.jdk.CollectionConverters.*

case class MetadataInfo(elementType: String = "", elementCount: Long = 0)

object MetadataReader:
  /**
   * Read metadata from a dataset repository.
   * Assumes that the metadata file is valid.
   * @param repoDir Path to the dataset repository
   * @return
   */
  def read(repoDir: Path): MetadataInfo =
    val model = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val rb = "https://riverbench.github.io/schema/dataset#"
    val types = model.listObjectsOfProperty(model.createProperty(rb + "hasStreamElementType"))
      .asScala.toSeq

    MetadataInfo(
      types.head.asResource.getURI.split('#').last,
      model.listObjectsOfProperty(model.createProperty(rb + "hasStreamElementCount"))
        .asScala.toSeq.head.asLiteral.getLong
    )
