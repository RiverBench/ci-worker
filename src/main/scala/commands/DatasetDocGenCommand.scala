package io.github.riverbench.ci_worker
package commands

import util.doc.{DocBuilder, DocFileUtil, MarkdownUtil}
import util.*

import org.apache.jena.rdf.model.Property
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

    DocFileUtil.copyDocs(datasetRepoDir.resolve("doc"), outputDir.resolve("docs"))

    val metadata = RDFDataMgr.loadModel(metadataPath.toString)
    val mi = MetadataReader.fromModel(metadata)
    val landingPage = mi.datasetRes.listProperties(RdfUtil.dcatLandingPage)
      .asScala.toSeq.head.getResource.getURI
    val version = mi.datasetRes.listProperties(RdfUtil.hasVersion)
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
        (1, RdfUtil.dcatDistribution),
        (1, RdfUtil.hasStatisticsSet),
      ),
      hidePropsInLevel = Seq(
        (1, RdfUtil.dctermsDescription), // shown as content below the header
        (1, RDF.`type`), // Always the same
        (3, RDF.`type`), // for distributions
      ),
      defaultPropGroup = Some("Technical metadata"),
      tabularProps = Seq(RdfUtil.hasStatistics),
    )
    val docBuilderIndex = new DocBuilder(ontologies, optIndex)
    val docIndex = docBuilderIndex.build(
      title,
      mi.description + indexIntro(mi, landingPage) + streamPreview(mi, version),
      mi.datasetRes
    )

    // Cheat: create a virtual statistics resource and create a section for it
    val m = mi.datasetRes.getModel
    val statsRes = m.createResource()
    for s <- m.listObjectsOfProperty(RdfUtil.hasStatisticsSet).asScala.distinct do
      statsRes.addProperty(RdfUtil.hasStatisticsSet, s)
    // val statSection = docIndex.addSubsection()
    docBuilderIndex.buildSection(statsRes, docIndex)

    // Save the index.md document
    Files.writeString(outputDir.resolve("docs/index.md"), docIndex.toMarkdown)
    println("Generated index.md")

    if version == "dev" then
      val optReadme = optIndex.copy(
        hidePropsInLevel = optIndex.hidePropsInLevel ++ Seq(
          (3, RdfUtil.hasStatisticsSet), // distribution stats
          (3, RdfUtil.spdxChecksum), // distribution checksums
        )
      )
      val docBuilderReadme = new DocBuilder(ontologies, optReadme)
      val docReadme = docBuilderReadme.build(title, mi.description + readmeIntro(landingPage), mi.datasetRes)
      Files.writeString(
        outputDir.resolve("README.md"),
        MarkdownUtil.readmeHeader("dataset-" + mi.identifier) + "\n\n" + docReadme.toMarkdown
      )
      println("Generated README.md")
    else
      println(f"Version is $version – not generating README.md")
  }

  private def readmeIntro(websiteLink: String): String =
    f"""
       |
       |*This README is a snapshot of documentation for the latest development version of the dataset.
       |Full documentation for all versions can be found [on the website]($websiteLink).*
       |""".stripMargin

  private def indexIntro(mi: MetadataInfo, websiteLink: String): String =
    f"""
       |
       |!!! info
       |
       |    Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(websiteLink)}
       |    <br>Source repository: **[${mi.identifier}](${Constants.baseRepoUrl}/dataset-${mi.identifier})**
       |
       |""".stripMargin

  private def streamPreview(mi: MetadataInfo, version: String): String =
    val extension = if mi.streamTypes.exists(_.elementType == ElementType.Triple) then "ttl" else "trig"
    val sb = StringBuilder("??? example \"Stream preview (click to expand)\"")
    for num <- Constants.streamSamples do
      sb.append("\n\n")
      sb.append(
        f"""    === "Element $num"
           |
           |        ```turtle title="$num%010d.$extension"
           |        --8<-- "docs/datasets/${mi.identifier}/$version/data/sample_$num%010d.$extension"
           |        ```
           |""".stripMargin
      )

    sb.toString()
