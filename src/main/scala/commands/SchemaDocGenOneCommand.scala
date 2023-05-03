package io.github.riverbench.ci_worker
package commands

import io.github.riverbench.ci_worker.util.AppConfig
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

// TODO: move this to the package schema command

object SchemaDocGenOneCommand extends Command:
  def name: String = "schema-doc-gen-1"

  def description: String = "Generates documentation for the schema repo (stage 1).\n" +
    "Args: <schema repo dir> <output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 3

  def run(args: Array[String]): Future[Unit] = Future {
    val schemaRepoDir = FileSystems.getDefault.getPath(args(1))
    val outDir = FileSystems.getDefault.getPath(args(2))

    genOwlOntology(schemaRepoDir.resolve("src/metadata.ttl"), outDir)
  }

  private def genOwlOntology(ontFile: Path, outDir: Path): Unit =
    val m = RDFDataMgr.loadModel(ontFile.toString)
    val externalSubjects = m.listSubjects().asScala
      .filter(_.isURIResource)
      .filter(!_.getURI.startsWith(AppConfig.CiWorker.rbRootUrl))
      .map(_.listProperties().asScala.toSeq)
      .filter(sts => sts.size == 1 && sts.head.getPredicate == RDF.`type`)
      .map(sts => sts.head.getSubject.asResource)
      .toSeq

    for s <- externalSubjects do
      m.removeAll(s, null, null)

    val os = new FileOutputStream(outDir.resolve(ontFile.getFileName).toFile)
    RDFDataMgr.write(os, m, RDFFormat.TURTLE)
    os.flush()
    os.close()