package io.github.riverbench.ci_worker
package util

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParser}
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.system.FactoryRDFStd

import java.nio.file.Path
import java.util.UUID

object RdfIoUtil:
  private val labelToNode = LabelToNode.createScopeByDocumentHash(
    UUID.fromString("13371337-1337-1337-1337-000000000000")
  )
  private val rdfFactory = FactoryRDFStd(labelToNode)
  
  def loadOntologies(schemaRepoDir: Path): Model =
    val ontologyPaths = Seq(
      schemaRepoDir.resolve("src/metadata.ttl"),
      schemaRepoDir.resolve("src/documentation.ttl"),
    ) ++ schemaRepoDir.resolve("src/imports").toFile.listFiles()
      .filter(f => f.isFile && (f.getName.endsWith(".ttl") || f.getName.endsWith(".rdf")))
      .map(_.toPath)

    val model = ModelFactory.createDefaultModel()
    for path <- ontologyPaths do
      val m = loadWithStableBNodeIds(path)
      m.removeNsPrefix("")
      model.add(m)
    model

  def loadWithStableBNodeIds(p: Path, lang: Lang = null): Model =
    val model = ModelFactory.createDefaultModel()
    RDFParser.create()
      .factory(rdfFactory)
      .source(p)
      .lang(lang)
      .parse(model)
    model
