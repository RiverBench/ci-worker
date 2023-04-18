package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr

import java.nio.file.Path

object RdfIoUtil:
  def loadOntologies(schemaRepoDir: Path): Model =
    val ontologyPaths = Seq(
      schemaRepoDir.resolve("src/dataset.ttl"),
      schemaRepoDir.resolve("src/documentation.ttl"),
      schemaRepoDir.resolve("src/theme.ttl"),
    ) ++ schemaRepoDir.resolve("src/imports").toFile.listFiles()
      .filter(f => f.isFile && (f.getName.endsWith(".ttl") || f.getName.endsWith(".rdf")))
      .map(_.toPath)

    val model = ModelFactory.createDefaultModel()
    for path <- ontologyPaths do
      val m = RDFDataMgr.loadModel(path.toString)
      m.removeNsPrefix("")
      model.add(m)
    model
