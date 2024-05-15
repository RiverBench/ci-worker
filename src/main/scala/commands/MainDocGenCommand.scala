package io.github.riverbench.ci_worker
package commands

import util.*
import util.doc.{DocBuilder, MarkdownUtil}

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS}

import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object MainDocGenCommand extends Command:
  def name: String = "main-doc-gen"

  def description: String = "Generates documentation for the main repo (incl. profiles).\n" +
    "Args: <version> <main repo dir> <main metadata out dir> <schema repo dir> <output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 6

  def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val mainMetadataOutDir = FileSystems.getDefault.getPath(args(3))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(4))
    val outDir = FileSystems.getDefault.getPath(args(5))

    println("Generating main documentation...")
    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    val mainMetadata = RDFDataMgr.loadModel(mainMetadataOutDir.resolve("metadata.ttl").toString)
    val mainDocOpt = DocBuilder.Options(
      titleProps = Seq(
        RdfUtil.hasLabelOverride,
        RdfUtil.dctermsTitle,
        RDFS.label,
        SKOS.prefLabel,
        RDF.`type`,
      ),
      hidePropsInLevel = Seq(
        (1, RdfUtil.dctermsDescription),
        (1, RdfUtil.dctermsTitle),
        (1, RDF.`type`),
      ) ++ (if version == "dev" then Seq(
        (1, RdfUtil.hasVersion),
      ) else Seq.empty)
    )

    val rootRes = mainMetadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.RiverBench).next.asResource
    val mainDocBuilder = new DocBuilder(ontologies, mainDocOpt)
    val baseLink = f"${AppConfig.CiWorker.rbRootUrl}v/$version"
    val dumpLink = f"${AppConfig.CiWorker.rbRootUrl}dumps/$version"
    val mainDoc = mainDocBuilder.build(
      "RiverBench" + (if version == "dev" then "" else f" ($version)"),
      Files.readString(repoDir.resolve("doc/index_body.md")) + rdfInfo(baseLink, Some(dumpLink)),
      rootRes
    )
    Files.writeString(
      outDir.resolve("index.md"),
      Files.readString(repoDir.resolve("doc/index_header.md")) ++ "\n" ++
        mainDoc.toMarkdown ++ "\n" ++
        Files.readString(repoDir.resolve("doc/index_footer.md"))
    )

    if version == "dev" then
      println("Generating README...")
      val readmeDocOpt = mainDocOpt.copy(hidePropsInLevel = mainDocOpt.hidePropsInLevel ++ Seq(
        (1, RdfUtil.hasVersion),
      ))
      val readmeDocBuilder = new DocBuilder(ontologies, readmeDocOpt)
      val readmeDoc = readmeDocBuilder.build(
        "RiverBench",
        Files.readString(repoDir.resolve("doc/readme_body.md")),
        rootRes
      )
      Files.writeString(
        outDir.resolve("README.md"),
        staticContentWarn + "\n" +
          Files.readString(repoDir.resolve("doc/readme_header.md")).strip + "\n\n" +
          readmeDoc.toMarkdown
      )
  }

  private val staticContentWarn =
    """
      |<!--
      |-- THIS FILE IS AUTOGENERATED. DO NOT EDIT.
      |-- Please edit: metadata.ttl, doc/readme_header.md, doc/readme_body.md
      |-- The documentation will be then regenerated by the CI.
      |-->
      |""".stripMargin.strip

  private def rdfInfo(baseLink: String, dumpLink: Option[String] = None): String =
    val s = f"""
       |
       |!!! info
       |
       |    Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(baseLink)}
       |
       |""".stripMargin
    dumpLink match
      case Some(link) => s + f"    A complete dump of all metadata in RiverBench is also available: " +
        MarkdownUtil.formatMetadataLinks(link, ".gz") + "\n\n"
      case None => s
