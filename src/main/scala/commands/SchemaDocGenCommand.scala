package io.github.riverbench.ci_worker
package commands

import util.AppConfig

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr

import java.nio.file.{FileSystems, Files, Path}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

object SchemaDocGenCommand extends Command:
  def name: String = "schema-doc-gen"

  def description: String = "Post-processes documentation for the schema repo.\n" +
    "Args: <version> <in dir> <out dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 4

  def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val inDir = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))

    val mdFiles = Files.list(inDir)
      .iterator()
      .asScala
      .filter(_.toString.endsWith(".md"))

    for mdFile <- mdFiles do
      println(f"Processing ${mdFile.getFileName}...")
      val ontFile = inDir.resolve(mdFile.getFileName.toString.replace(".md", ".ttl"))
      val outFile = outDir.resolve(mdFile.getFileName.toString)
      val m = RDFDataMgr.loadModel(ontFile.toString)
      postProcessFile(m, version, mdFile, outFile)
  }

  private val rxUri = """(?sm)\* \*\*URI\*\*.*?`(.*?)`""".r
  private val rxListInd1 = """(?m)^ {2}\*""".r
  private val rxConceptList = """\) \(con\)\s+\* \[""".r
  private val rxOverview = """(?sm)^## Overview.*?overview""".r
  private val rxLi = """(?m)^<li>(.*)</li>""".r
  private val rxAnchorMess = """(?sm)(\[]\((.*?)\)\n)?### ([^\n]*?)\n([^\n]*)\n([^\n]*)\nURI \| `(.*?)`""".r
  private val rxHeading = """(?m)^(#{2,3}) """.r
  private val rxIndividualsFix = """(?sm)^\* \*\*Class\(es\)\*\*\n.*?(\[.*?)\n""".r
  private val rxRdfLinks = """(?sm)^\* \*\*(Ontology|Taxonomy) RDF.*?\n\n""".r
  private val rxTocFix = """]\(#(object|datatype|named)(properties|individuals)\)""".r
  private val rxGenerator = """(?m)(documentation created by .*)\n""".r
  private val rxPrefixes = """(?sm)^\* \*\*([a-z0-9-_]+?)\*\*\n( +)\* `(.+?)`\n""".r

  private def postProcessFile(model: Model, version: String, inPath: Path, outPath: Path): Unit =
    var c = Files.readString(inPath)

    val name = inPath.getFileName.toString.split('.').head
    // val mainUri = rxUri.findFirstMatchIn(c).get.group(1).split('#').head

    // (uri, name, old anchor, new anchor)
    val anchors: mutable.ArrayBuffer[(String, String, String, String)] = mutable.ArrayBuffer()
    c = rxAnchorMess.replaceAllIn(c, m => {
      val name = m.group(3).split('<').head.strip
      val oldAnchor = Option(m.group(2))
        .getOrElse(name.replace(" ", ""))
      val uri = m.group(6)
      // maybe skip if uri is external???
      val uriSplit = uri.split('#')
      val newAnchor = (if uriSplit.length > 1 then Some(uriSplit.last) else None)
        .getOrElse(oldAnchor)
      anchors += ((uri, name, oldAnchor, newAnchor))

      f"""### $name <a name=\"$newAnchor\"></a>
         |${m.group(4)}
         |${m.group(5)}
         |URI | `$uri`
         |""".stripMargin.strip
    })

    for (uri, name, oldAnchor, newAnchor) <- anchors do
      val rxAn = ("(?m)\\]\\(#" + Regex.quote(oldAnchor) + "\\)").r
      c = rxAn.replaceAllIn(c, f"](#$newAnchor)")

      val uriQuoted = Regex.quote(uri)
      val rxU = f"""\\[$uriQuoted]\\($uriQuoted\\)""".r
      c = rxU.replaceAllIn(c, f"[$name](#$newAnchor)")
      val rxU2 = f"\\[[^]]*]\\($uriQuoted\\)".r
      c = rxU2.replaceAllIn(c, f"[$name](#$newAnchor)")

    c = rxListInd1.replaceAllIn(c, "    *")
    c = rxConceptList.replaceAllIn(c, ") (con)\n* [")
    c = rxOverview.replaceAllIn(c, "")
    c = rxLi.replaceAllIn(c, "* $1")
    c = rxHeading.replaceAllIn(c, "\n$1 ")
    c = rxIndividualsFix.replaceAllIn(c, "Class(es) | $1\n")
    c = rxTocFix.replaceAllIn(c, "](#$1-$2)")
    c = rxGenerator.replaceAllIn(c, "$1 and [RiverBench CI worker](https://github.com/RiverBench/ci-worker)")

    c = rxPrefixes.replaceAllIn(c, m => {
      val prefix = m.group(1)
      val uri = m.group(3)
      if prefix.startsWith("default") || model.getNsURIPrefix(uri) != null then
        m.toString
      else
        ""
    })

    val baseLink = s"${AppConfig.CiWorker.rbRootUrl}schema/$name/$version"
    val rdfLinks = f"**[Turtle]($baseLink.ttl)**, **[N-Triples]($baseLink.nt)**, **[RDF/XML]($baseLink.rdf)**"
    c = rxRdfLinks.replaceAllIn(c, m => {
      val schemaType = m.group(1).toLowerCase
      f"""
         |
         |!!! info
         |
         |    Download this $schemaType in RDF: $rdfLinks
         |
         |
         |""".stripMargin
    })

    Files.writeString(outPath, c)