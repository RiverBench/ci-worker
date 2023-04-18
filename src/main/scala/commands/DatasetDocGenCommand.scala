package io.github.riverbench.ci_worker
package commands

import util.{MetadataReader, RdfIoUtil, RdfUtil}
import util.doc.DocBuilder

import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.{RDF, RDFS}

import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object DatasetDocGenCommand extends Command:
  def name: String = "dataset-doc-gen"

  def description: String = "Generates documentation for a dataset.\n" +
    "Args: <path to merged metadata.ttl> <dataset repo dir> <schema repo dir> <output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 5

  def run(args: Array[String]): Future[Unit] = Future {
    val metadataPath = FileSystems.getDefault.getPath(args(1))
    val datasetRepoDir = FileSystems.getDefault.getPath(args(2))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(3))
    val outputDir = FileSystems.getDefault.getPath(args(4))

    copyDocs(datasetRepoDir, outputDir)

    val metadata = RDFDataMgr.loadModel(metadataPath.toString)
    val mi = MetadataReader.fromModel(metadata)
    val datasetRes = metadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    val landingPage = datasetRes.listProperties(RdfUtil.dcatLandingPage)
      .asScala.toSeq.head.getResource.getURI
    val version = datasetRes.listProperties(RdfUtil.hasVersion)
      .asScala.toSeq.head.getLiteral.getString

    val readableVersion = version match
      case "dev" => "development version"
      case _ => version
    val title = f"${mi.identifier} ($readableVersion)"

    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    val optIndex = DocBuilder.Options(
      titleProps = Seq(
        RdfUtil.dctermsTitle,
        RDFS.label,
        FOAF.name,
        FOAF.nick,
        RDF.`type`,
      ),
      nestedSectionProps = Seq(
        RdfUtil.dcatDistribution,
        RdfUtil.hasStatistics,
      ),
      hidePropsInLevel = Seq(
        (1, RdfUtil.dctermsDescription), // shown as content below the header
        (1, RDF.`type`), // Always the same
        (3, RDF.`type`), // for distributions
      )
    )
    val docBuilderIndex = new DocBuilder(ontologies, optIndex)
    val docIndex = docBuilderIndex.build(title, mi.description, datasetRes)
    Files.writeString(outputDir.resolve("docs/index.md"), docIndex.toMarkdown)
    println("Generated index.md")

    if version == "dev" then
      val optReadme = optIndex.copy(
        hidePropsInLevel = optIndex.hidePropsInLevel ++ Seq(
          (3, RdfUtil.hasStatistics), // distribution stats
          (3, RdfUtil.spdxChecksum), // distribution checksums
        )
      )
      val docBuilderReadme = new DocBuilder(ontologies, optReadme)
      val docReadme = docBuilderReadme.build(title, mi.description + readmeIntro(landingPage), datasetRes)
      Files.writeString(
        outputDir.resolve("README.md"),
        readmeHeader(mi.identifier) + "\n\n" + docReadme.toMarkdown
      )
      println("Generated README.md")
    else
      println(f"Version is $version – not generating README.md")
  }

  private val allowedDocExtensions = Set("md", "jpg", "png", "svg", "jpeg", "bmp", "webp", "gif")

  private def copyDocs(datasetDir: Path, outputDir: Path): Unit =
    val docDir = datasetDir.resolve("doc")
    val outputDocDir = outputDir.resolve("docs")
    Files.createDirectories(outputDocDir)
    if Files.exists(docDir) then
      val docFiles = Files.list(docDir).iterator().asScala
        .filter(f => Files.isRegularFile(f) && allowedDocExtensions.contains(f.getFileName.toString.split('.').last))
        // Only files smaller than 2 MB
        .filter(f => Files.size(f) < 2 * 1024 * 1024)
        .toSeq
      for docFile <- docFiles do
        val target = outputDocDir.resolve(docFile.getFileName)
        Files.copy(docFile, target)
        println(s"Copied $docFile to $target")

  private val baseRepoUrl = "https://github.com/RiverBench"

  private def readmeHeader(id: String): String =
    f"""<!--
       |--
       |-- THIS FILE IS AUTOGENERATED. DO NOT EDIT.
       |-- Please edit the metadata.ttl file instead. The documentation
       |-- will be regenerated by the CI.
       |--
       |-- You can place additional docs in the /doc directory. Remember to link
       |-- to them from the description in the metadata.ttl file.
       |--
       |-->
       |[![.github/workflows/release.yaml]($baseRepoUrl/dataset-$id/actions/workflows/release.yaml/badge.svg?event=push)]($baseRepoUrl/dataset-$id/actions/workflows/release.yaml)
       |""".stripMargin

  private def readmeIntro(websiteLink: String): String =
    f"""
       |
       |*This README is a snapshot of documentation for the latest development version of the dataset.
       |Full documentation for all versions can be found [on the website]($websiteLink).*
       |""".stripMargin

