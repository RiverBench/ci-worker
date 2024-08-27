package io.github.riverbench.ci_worker
package commands

import util.doc.{DocBuilder, DocFileUtil, MarkdownUtil}
import util.*

import org.apache.jena.rdf.model.{Property, RDFNode}
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

    val metadata = RdfIoUtil.loadModelWithStableBNodeIds(metadataPath)
    val mi = MetadataReader.fromModel(metadata)
    val landingPage = mi.datasetRes.listProperties(RdfUtil.dcatLandingPage)
      .asScala.toSeq.head.getResource.getURI
    val version = mi.datasetRes.listProperties(RdfUtil.hasVersion)
      .asScala.toSeq.head.getLiteral.getString

    val readableVersion = version match
      case "dev" => "development version"
      case _ => version
    val title = f"Dataset: ${mi.identifier} ($readableVersion)"

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
      customSectionContentGen = Map(
        RdfUtil.dcatDistribution -> generateDownloadTable,
      ),
    )
    val docBuilderIndex = new DocBuilder(ontologies, optIndex)
    val docIndex = docBuilderIndex.build(
      title,
      mi.description + indexIntro(mi, landingPage) + streamPreview(mi),
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
    val purl = PurlMaker.unMake(landingPage).get
    Files.writeString(
      outputDir.resolve("docs/index.md"),
      MarkdownUtil.makeTopButtons(purl, fileDepth = 2) + "\n\n" + docIndex.toMarkdown
    )
    println("Generated index.md")

    if version == "dev" then
      val optReadme = optIndex.copy(
        hidePropsInLevel = optIndex.hidePropsInLevel ++ Seq(
          (3, RdfUtil.hasStatisticsSet), // distribution stats
          (3, RdfUtil.spdxChecksum), // distribution checksums
        ),
        customSectionContentGen = Map(),
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

  private def generateDownloadTable(distributions: Seq[RDFNode]): String =
    val data = distributions.map { dist =>
      val distRes = dist.asResource()
      val downloadLink = distRes.listProperties(RdfUtil.dcatDownloadURL).next.getResource.getURI
      val id = distRes.listProperties(RdfUtil.dctermsIdentifier).next.getLiteral.getString.split('-')
      val row = id.last
      val distType = id.head
      val byteSize = distRes.listProperties(RdfUtil.dcatByteSize).next.getLiteral.getLong
      val elementCount = distRes.listProperties(RdfUtil.hasStreamElementCount).next.getLiteral.getLong
      val stats = distRes.getProperty(RdfUtil.hasStatisticsSet).getResource
      val statements = stats.listProperties(RdfUtil.hasStatistics).asScala
        .map(_.getResource)
        .filter(_.hasProperty(RDF.`type`, RdfUtil.StatementCountStatistics))
        .map(_.getProperty(RdfUtil.sum).getLong)
        .toSeq.head

      (distType, row, downloadLink, byteSize, elementCount, statements)
    }
    val rows = data
      .groupBy(_._2)
      .map((row, dists) => dists.head).toSeq
      .sortBy(_._5)
    val sb = StringBuilder()
    sb.append(
      """
        |### Download links
        |
        |The dataset is published in a few size variants, each containing a specific number of stream elements.
        |For each size, there are three distribution types available: flat (just an N-Triples/N-Quads file),
        |streaming (a .tar.gz archive with Turtle/TriG files, one file per stream element),
        |and [Jelly](https://w3id.org/jelly) (a native binary format for streaming RDF).
        |See the [documentation](../../documentation/dataset-release-format.md) for more details.
        |
        |""".stripMargin)
    sb.append("Distribution size | Statements | Flat | Streaming | Jelly\n")
    sb.append("--- | --: | --: | --: | --:\n")
    for row <- rows do
      val dists = data.filter(_._2 == row._2)
      val hover = f"${MarkdownUtil.formatInt(row._5.toString)} stream elements"
      sb.append(f"<abbr title=\"$hover\">${Constants.packageSizeToHuman(row._5, true)}</abbr>")
      sb.append(" | ")
      sb.append(MarkdownUtil.formatInt(row._6.toString))
      for dType <- Seq("flat", "stream", "jelly") do
        sb.append(" | ")
        val dist = dists.find(_._1 == dType).get
        sb.append(f"[:octicons-download-24: ${MarkdownUtil.formatSize(dist._4)}](${dist._3})")
      sb.append("\n")
    sb.append("\n\nThe full metadata of all distributions can be found below.")
    sb.toString()

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
       |    :fontawesome-solid-diagram-project: Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(websiteLink)}
       |    <br>:material-github: Source repository: **[dataset-${mi.identifier}](${Constants.baseRepoUrl}/dataset-${mi.identifier})**
       |    <br>${MarkdownUtil.formatPurlLink(websiteLink)}
       |
       |    **[:octicons-arrow-down-24: Go to download links](#distributions)**
       |
       |""".stripMargin

  private def streamPreview(mi: MetadataInfo): String =
    val extension = if mi.streamTypes.exists(_.elementType == ElementType.Triple) then "ttl" else "trig"
    val sb = StringBuilder("??? example \"Stream preview (click to expand)\"")
    for num <- Constants.streamSamples do
      sb.append("\n\n")
      sb.append(
        f"""    === "Element $num"
           |
           |        ```turtle title="$num%010d.$extension"
           |        --8<-- "docs/datasets/${mi.identifier}/data/sample_$num%010d.$extension"
           |        ```
           |""".stripMargin
      )

    sb.toString()
