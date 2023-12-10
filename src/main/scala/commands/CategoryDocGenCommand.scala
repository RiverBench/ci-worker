package io.github.riverbench.ci_worker
package commands

import util.*
import util.doc.*

import org.apache.jena.rdf.model.{Model, Property, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.{RDF, RDFS}

import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object CategoryDocGenCommand extends Command:
  def name: String = "category-doc-gen"

  def description: String = "Generates documentation for a dataset.\n" +
    "Args: <package out dir> <repo dir> <schema repo dir> <doc output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 5

  private val catTaskDocOpt = DocBuilder.Options(
    titleProps = Seq(
      RdfUtil.dctermsTitle,
      RDFS.label,
      RDF.`type`,
    ),
    hidePropsInLevel = Seq(
      (1, RDF.`type`), // Always the same
    ),
    defaultPropGroup = Some("General information"),
    startHeadingLevel = 2,
  )

  def run(args: Array[String]): Future[Unit] = Future {
    val packageOutDir = FileSystems.getDefault.getPath(args(1))
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(3))
    val outDir = FileSystems.getDefault.getPath(args(4))

    val catM = RDFDataMgr.loadModel(packageOutDir.resolve("category/metadata.ttl").toString)
    val catRes = catM.listSubjectsWithProperty(RDF.`type`, RdfUtil.Category).next.asResource
    val version = RdfUtil.getString(catRes, RdfUtil.hasVersion).get

    println("Generating profile documentation...")
    val profileCollection = new ProfileCollection(packageOutDir.resolve("profiles"))
    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    profileDocGen(profileCollection, ontologies, packageOutDir, outDir, version)
    profileOverviewDocGen(version, profileCollection, outDir)

    println("Generating task documentation...")
    taskDocGen(ontologies, repoDir, packageOutDir, outDir, version)

    println("Generating category documentation...")
    categoryDocGen(version, catRes, ontologies, repoDir, outDir)
  }

  private def categoryDocGen(version: String, catRes: Resource, ontologies: Model, repoDir: Path, outDir: Path): Unit =
    val title = RdfUtil.getString(catRes, RdfUtil.dctermsTitle).get
    val description = Files.readString(repoDir.resolve("doc/index.md")) + "\n\n## Metadata\n"
    val builder = new DocBuilder(ontologies, catTaskDocOpt)
    val catDoc = builder.build(
      title,
      description + rdfInfo(s"${AppConfig.CiWorker.baseCategoryUrl}$version"),
      catRes
    )
    val targetDir = outDir.resolve("category")
    targetDir.toFile.mkdirs()
    Files.writeString(targetDir.resolve("index.md"), catDoc.toMarkdown)
    DocFileUtil.copyDocs(repoDir.resolve("doc"), targetDir, Seq("index.md"))


  private def taskDocGen(ontologies: Model, repoDir: Path, metadataOutDir: Path, outDir: Path, version: String): Unit =
    val taskDocBuilder = new DocBuilder(ontologies, catTaskDocOpt)
    repoDir.resolve("tasks").toFile.listFiles()
      .filter(_.isDirectory)
      .foreach(f => {
        val taskName = f.getName
        val taskM = RDFDataMgr.loadModel(metadataOutDir.resolve(f"tasks/task-$taskName.ttl").toString)
        val taskRes = taskM.listSubjectsWithProperty(RDF.`type`, RdfUtil.Task).next.asResource
        val title = RdfUtil.getString(taskRes, RdfUtil.dctermsTitle) getOrElse taskName

        val description = Files.readString(f.toPath.resolve("index.md")) + "\n\n## Metadata\n"
        val taskDoc = taskDocBuilder.build(
          title,
          description + rdfInfo(s"${AppConfig.CiWorker.baseTaskUrl}$taskName/$version"),
          taskRes
        )
        val targetDir = outDir.resolve(f"tasks/$taskName")
        targetDir.toFile.mkdirs()
        Files.writeString(targetDir.resolve("index.md"), taskDoc.toMarkdown)

        DocFileUtil.copyDocs(f.toPath, targetDir, Seq("index.md"))
      })

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

  private def profileOverviewDocGen(version: String, profileCollection: ProfileCollection, outDir: Path): Unit =
    def taxoLink(text: String, name: String) =
      f"[$text](${Constants.taxonomyDocBaseLink}$name-stream)"

    val sb = new StringBuilder()
    sb.append("Profile | Stream type | RDF-star | Non-standard extensions\n")
    sb.append("--- | --- | :-: | :-:\n")
    for pName <- profileCollection.profiles.keys.toSeq.sorted do
      val nameSplit = pName.split('-')
      sb.append(f"[$pName](${AppConfig.CiWorker.baseProfileUrl}$pName/$version) | ")
      sb.append(
        (nameSplit(0), nameSplit(1)) match
          case ("flat", "mixed") => "flat " + taxoLink("triple", "flat-rdf-triple") +
            " or " + taxoLink("quad", "flat-rdf-quad")
          case ("flat", t) => taxoLink("flat " + t.dropRight(1), "flat-rdf-" + t.dropRight(1))
          case ("stream", "mixed") => taxoLink("dataset", "rdf-dataset") + " or " +
            taxoLink("graph", "rdf-graph")
          case ("stream", "datasets") => taxoLink("dataset", "rdf-dataset")
          case ("stream", "named") => taxoLink("named graph", "rdf-named-graph")
          case ("stream", "ts") => taxoLink("timestamped named graph", "timestamped-rdf-named-graph")
          case ("stream", "subject") => taxoLink("subject graph", "rdf-subject-graph")
          case _ => taxoLink("graph", "rdf-graph")
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

  private def rdfInfo(baseLink: String): String =
    f"""
       |
       |!!! info
       |
       |    Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(baseLink)}
       |
       |""".stripMargin