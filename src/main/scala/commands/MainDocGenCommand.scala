package io.github.riverbench.ci_worker
package commands

import util.{AppConfig, ProfileCollection, RdfIoUtil, RdfUtil}
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

    println("Generating profile documentation...")
    val profileCollection = new ProfileCollection(mainMetadataOutDir.resolve("profiles"))
    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    profileDocGen(profileCollection, ontologies, mainMetadataOutDir, outDir, version)
    if version == "dev" then
      profileOverviewDocGen(profileCollection, outDir)

    println("Generating main documentation...")
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
    val mainDoc = mainDocBuilder.build(
      "RiverBench" + (if version == "dev" then "" else f" ($version)"),
      Files.readString(repoDir.resolve("doc/index_body.md")) + rdfInfo(baseLink),
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

  private def profileDocGen(
    profileCollection: ProfileCollection, ontologies: Model, metadataOutDir: Path, outDir: Path, version: String
  ):
  Unit =
    val profileDocOpt = DocBuilder.Options(
      titleProps = Seq(
        RdfUtil.dctermsTitle,
        RDFS.label,
        RDF.`type`,
      ),
      hidePropsInLevel = Seq(
        (1, RdfUtil.dctermsDescription), // shown as content below the header
        (1, RDF.`type`), // Always the same
      ),
      defaultPropGroup = Some("General information"),
    )
    val profileDocBuilder = new DocBuilder(ontologies, profileDocOpt)
    outDir.resolve("profiles").toFile.mkdirs()

    for (name, profile) <- profileCollection.profiles do
      val profileRes = profile.listSubjectsWithProperty(RDF.`type`, RdfUtil.Profile).next.asResource
      val description = RdfUtil.getString(profileRes, RdfUtil.dctermsDescription) getOrElse ""
      val profileDoc = profileDocBuilder.build(
        s"$name (${readableVersion(version)})",
        description + rdfInfo(s"${AppConfig.CiWorker.baseProfileUrl}$name/$version"),
        profileRes
      )
      val tableSection =
        """
          |## Download links
          |
          |Below you will find links to download the profile's datasets in different lengths.
          |
          |!!! warning
          |    Some datasets are shorter than others and a given distribution may not be available for all datasets.
          |    In that case, a link to the longest available distribution of the dataset is provided.
          |
          |""".stripMargin +
        Files.readString(metadataOutDir.resolve(f"profiles/doc/${name}_table.md"))
      val profileDocPath = outDir.resolve(s"profiles/$name.md")
      Files.writeString(profileDocPath, profileDoc.toMarkdown + tableSection)

  private def profileOverviewDocGen(profileCollection: ProfileCollection, outDir: Path): Unit =
    val sb = new StringBuilder()
    sb.append("Profile | Stream / flat | Element type | RDF-star | Non-standard extensions\n")
    sb.append("--- | --- | --- | :-: | :-:\n")
    for pName <- profileCollection.profiles.keys.toSeq.sorted do
      val nameSplit = pName.split('-')
      sb.append(f"[$pName]($pName/dev) | ")
      sb.append(nameSplit.head)
      sb.append(" | ")
      sb.append(
        (nameSplit(0), nameSplit(1)) match
        case ("flat", "mixed") => "triple, quad"
        case ("flat", t) => t.dropRight(1)
        case ("stream", "mixed") => "triples, quads"
        case ("stream", t) => t
      )
      for restriction <- Seq("rdfstar", "nonstandard") do
        sb.append(" | ")
        sb.append(
          if pName.contains(restriction) then ":material-check:"
          else ":material-close:"
        )
      sb.append("\n")

    Files.writeString(outDir.resolve("profiles/table.md"), sb.toString())

  private def readableVersion(v: String) =
    if v == "dev" then "development version" else v

  private val staticContentWarn =
    """
      |<!--
      |-- THIS FILE IS AUTOGENERATED. DO NOT EDIT.
      |-- Please edit: metadata.ttl, doc/readme_header.md, doc/readme_body.md
      |-- The documentation will be then regenerated by the CI.
      |-->
      |""".stripMargin.strip

  private def rdfInfo(baseLink: String): String =
    f"""
       |
       |!!! info
       |
       |    Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(baseLink)}
       |
       |""".stripMargin
