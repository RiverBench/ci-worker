package io.github.riverbench.ci_worker
package commands

import util.RdfUtil
import util.doc.DatasetDocBuilder
import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.{RDF, RDFS}

import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future

object DatasetDocGenCommand extends Command:
  def name: String = "dataset-doc-gen"

  def description: String = "Generates documentation for a dataset.\n" +
    "Args: <path to merged metadata.ttl> <schema repo dir> <output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 4

  def run(args: Array[String]): Future[Unit] = Future {
    val metadataPath = FileSystems.getDefault.getPath(args(1))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(2))
    val outputDir = FileSystems.getDefault.getPath(args(3))

    val metadata = RDFDataMgr.loadModel(metadataPath.toString)

    val ontologies = getOntologies(schemaRepoDir)
    val docBuilder = new DatasetDocBuilder(ontologies, metadata)
    val doc = docBuilder.build()

    println(doc.toMarkdown)
  }

  private def getOntologies(schemaRepoDir: Path): Model =
    val ontologyPaths = Seq(
      schemaRepoDir.resolve("src/dataset.ttl"),
      schemaRepoDir.resolve("src/documentation.ttl"),
      schemaRepoDir.resolve("src/theme.ttl"),
    ) ++ schemaRepoDir.resolve("src/imports").toFile.listFiles()
      .filter(f => f.isFile && (f.getName.endsWith(".ttl") || f.getName.endsWith(".rdf")))
      .map(_.toPath)

    val model = ModelFactory.createDefaultModel()
    for path <- ontologyPaths do
      RDFDataMgr.read(model, path.toString)
    model


