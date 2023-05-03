package io.github.riverbench.ci_worker
package commands

import util.AppConfig

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

object SchemaDocGenTwoCommand extends Command:
  def name: String = "schema-doc-gen-2"

  def description: String = "Generates documentation for the schema repo (stage 2).\n" +
    "Args: <ont file> <version> <in file> <out file>"

  def validateArgs(args: Array[String]): Boolean = args.length == 5

  def run(args: Array[String]): Future[Unit] = Future {
    val ontFile = FileSystems.getDefault.getPath(args(1))
    val version = args(2)
    val inFile = FileSystems.getDefault.getPath(args(3))
    val outFile = FileSystems.getDefault.getPath(args(4))

    val m = RDFDataMgr.loadModel(ontFile.toString)

    postProcessFile(m, version, inFile, outFile)
  }

  private val rxListInd1 = """(?m)^ {2}\*""".r
  private val rxOverview = """(?sm)^## Overview.*?overview""".r
  private val rxLi = """(?m)^<li>(.*)</li>""".r
  private val rxHeading = """(?m)^(#{2,3}) """.r
  private val rxIndividualsFix = """(?sm)^\* \*\*Class\(es\)\*\*\n.*?(\[.*?)\n""".r
  private val rxRdfLinks = """(?sm)^\* \*\*Ontology RDF.*?\n\n""".r

  private def postProcessFile(m: Model, version: String, inPath: Path, outPath: Path): Unit =
    val name = inPath.getFileName.toString.split('.').head

    var c = Files.readString(inPath)
    // TODO: anchors
    // TODO: ontology rdf links, prettier
    // TODO: non-prefixed URIs to prettier beasts
    // TODO: remove superfluous prefixes
    c = rxListInd1.replaceAllIn(c, "    *")
    c = rxOverview.replaceAllIn(c, "")
    c = rxLi.replaceAllIn(c, "* $1")
    c = rxHeading.replaceAllIn(c, "\n$1 ")
    c = rxIndividualsFix.replaceAllIn(c, "Class(es) | $1\n")

    val baseLink = s"${AppConfig.CiWorker.rbRootUrl}schema/$name/$version"
    val rdfLinks = f"**[Turtle]($baseLink.ttl)**, **[N-Triples]($baseLink.nt)**, **[RDF/XML]($baseLink.rdf)**"
    val linksNotice =
      f"""
         |
         |!!! info
         |
         |    Download this ontology in RDF: $rdfLinks
         |
         |
         |""".stripMargin
    c = rxRdfLinks.replaceFirstIn(c, linksNotice)

    Files.writeString(outPath, c)