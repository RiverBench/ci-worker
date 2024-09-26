package io.github.riverbench.ci_worker
package commands

import util.*
import util.SemVer.*
import util.collection.*
import util.doc.*
import util.external.*

import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource}
import org.apache.jena.vocabulary.{RDF, RDFS}

import java.io.File
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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
      (2, RDF.`type`), // Always the same
    ),
    defaultPropGroup = Some("General information"),
    startHeadingLevel = 2,
  )

  // Documentation formatter for system under test
  private case class DocValueSut(name: String, version: Option[String]) extends DocValue:
    override def toMarkdown: String = name + version.fold("")(v => s" ($v)")
    override def getSortKey: String = toMarkdown

  private val benchResultsDocOpt = DocBuilder.Options(
    hidePropsInLevel = Seq(
      (3, RDF.`type`),
    ),
    startHeadingLevel = 3,
    customValueFormatters = {
      case (_, res: Resource) if {
        val props = res.listProperties().asScala.map(_.getPredicate).toSeq
        res.hasProperty(RDF.`type`, RdfUtil.SystemUnderTest) && props.contains(RDFS.label)
      } =>
        val name = res.getProperty(RDFS.label).getObject.asLiteral().getString
        // rb:hasVersion was used until 2.2.0, from 2.2.0 onwards dcat:version is used
        // Read both to support older versions!
        val version = (res.listProperties(RdfUtil.hasVersion).asScala ++
          res.listProperties(RdfUtil.dcatVersion).asScala)
          .toSeq.headOption
          .map(_.getObject.asLiteral().getString)
        DocValueSut(name, version)
    },
    defaultPropGroup = Some("General information"),
  )

  private val allowedPubInfoPredicates = Set(
    RdfUtil.dctermsCreator, RdfUtil.dctermsLicense, RdfUtil.dctermsCreated,
  )

  def run(args: Array[String]): Future[Unit] = Future {
    val packageOutDir = FileSystems.getDefault.getPath(args(1))
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(3))
    val outDir = FileSystems.getDefault.getPath(args(4))

    val catM = RdfIoUtil.loadModelWithStableBNodeIds(packageOutDir.resolve("category/metadata.ttl"))
    val catRes = catM.listSubjectsWithProperty(RDF.`type`, RdfUtil.Category).next.asResource
    val version = RdfUtil.getString(catRes, RdfUtil.dcatVersion).get

    outDir.resolve("category").toFile.mkdirs()

    println("Generating profile documentation...")
    val profileCollection = new ProfileCollection(packageOutDir.resolve("profiles"))
    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    profileDocGen(profileCollection, ontologies, packageOutDir, outDir, version)
    profileOverviewDocGen(version, profileCollection, outDir)

    println("Generating task documentation...")
    val tasks = taskDocGen(ontologies, repoDir, packageOutDir, outDir, version)
    taskOverviewDocGen(version, tasks, outDir)

    println("Generating category documentation...")
    categoryDocGen(version, catRes, ontologies, repoDir, outDir)
  }

  private def categoryDocGen(version: String, catRes: Resource, ontologies: Model, repoDir: Path, outDir: Path): Unit =
    val id = RdfUtil.getString(catRes, RdfUtil.dctermsIdentifier).get
    val title = RdfUtil.getString(catRes, RdfUtil.dctermsTitle).get
    val tableSections =
      f"""
        |## Benchmark tasks
        |
        |Below you will find a list of tasks that are part of this benchmark category.
        |
        |!!! tip
        |
        |    Benchmark tasks and profiles in RiverBench have machine-readable metadata in RDF.
        |    You can find RDF download links for each profile on its documentation page.
        |    You can also use the [HTTP content negotiation mechanism](../../documentation/metadata.md).
        |
        |--8<-- "docs/categories/$id/task_table.md"
        |
        |<div style="text-align: center" markdown>[:material-invoice-text-plus: Propose a new benchmark task](../../documentation/creating-new-task.md){ .md-button }</div>
        |
        |## Benchmark profiles
        |
        |Profiles in RiverBench group datasets that share common technical characteristics.
        |For example, whether the datasets consist of triples or quads, if they use RDF-star, etc.
        |The profiles are intended to be used in benchmarks to compare the performance of different systems on a well-defined collection of datasets.
        |
        |See the **[quick start guide](../../documentation/using.md)** for more information on how to use the profiles, tasks, and datasets.
        |
        |--8<-- "docs/categories/$id/profile_table.md"
        |
        |""".stripMargin

    val description = RdfUtil.getString(catRes, RdfUtil.dctermsDescription).get
    val catOpt = catTaskDocOpt.copy(
      hidePropsInLevel = catTaskDocOpt.hidePropsInLevel ++ Seq((2, RdfUtil.dctermsDescription))
    )
    val builder = new DocBuilder(ontologies, catOpt)
    val catPurl = PurlMaker.Purl(id, version, "categories")
    val catDoc = builder.build(
      "Metadata",
      rdfInfo(catPurl),
      catRes
    )
    val targetDir = outDir.resolve("category")
    Files.writeString(
      targetDir.resolve("index.md"),
      f"""${MarkdownUtil.makeTopButtons(catPurl, fileDepth = 2)}
         |
         |# $title
         |
         |$description$tableSections
         |
         |""".stripMargin + catDoc.toMarkdown
    )
    DocFileUtil.copyDocs(repoDir.resolve("doc"), targetDir, Seq("index.md"))

    // Do the README
    if version == "dev" then
      println("Generating README...")
      val docReadme = builder.build("Metadata", "", catRes)
      val websiteLink = PurlMaker.category(id, version)
      val readmeIntro =
        f"""# $title
           |
           |${MarkdownUtil.readmeHeader("category-" + id)}
           |
           |*This README is a snapshot of documentation for the latest development version of the benchmark category.
           |Full documentation for all versions can be found [on the website]($websiteLink).*
           |
           |$description
           |
           |""".stripMargin
      Files.writeString(
        targetDir.resolve("README.md"),
        readmeIntro + docReadme.toMarkdown
      )
    else
      print(f"Version is $version, not generating README.")


  private def taskDocGen(ontologies: Model, repoDir: Path, metadataOutDir: Path, outDir: Path, version: String):
  Seq[(String, Model)] =
    val dumpDataset = RdfIoUtil.loadDatasetWithStableBNodeIds(metadataOutDir.resolve("dump.jelly"))
    val models = dumpDataset.listNames().asScala
      .map(graphName => dumpDataset.getNamedModel(graphName))
    val potentialDois = DoiBibliography.getPotentialDois(models)
    val preloadBibFuture = DoiBibliography.preloadBibliography(potentialDois)
    val taskDocBuilder = new DocBuilder(ontologies, catTaskDocOpt)
    val benchResultsDocBuilder = new DocBuilder(ontologies, benchResultsDocOpt)

    def genOne(f: File): (String, Model) =
      // Prepare data for the task documentation
      val taskName = f.getName
      val taskVersion = Version(version)
      val taskM = RdfIoUtil.loadModelWithStableBNodeIds(metadataOutDir.resolve(f"tasks/task-$taskName.ttl"))
      val taskRes = taskM.listSubjectsWithProperty(RDF.`type`, RdfUtil.Task).next.asResource
      val title = f"Task: ${RdfUtil.getString(taskRes, RdfUtil.dctermsTitle) getOrElse taskName} " +
        f"(${readableVersion(version)})"
      val description = Files.readString(f.toPath.resolve("index.md"))
      val taskPurl = PurlMaker.Purl(taskName, version, "tasks")

      val taskDoc = taskDocBuilder.build(
        "Metadata",
        rdfInfo(taskPurl),
        taskRes
      )

      // Process benchmark result Nanopublications
      // Tuple: (nanopub URI, pubinfo graph URI, label)
      val benchmarkNodeToNanopubInfo = scala.collection.mutable.Map[String, (Resource, Resource, String)]()
      val nanopubUriToLabel = scala.collection.mutable.Map[String, String]()
      val benchmarkNodes = dumpDataset.listNames().asScala.flatMap(graphName => {
        val m = dumpDataset.getNamedModel(graphName)
        m.listStatements(null, RdfUtil.npHasAssertion, null).asScala.foreach(st => {
          val npUri = st.getSubject.asResource()
          val pubInfoGraphName = st.getSubject.getProperty(RdfUtil.npHasPublicationInfo).getObject.asResource()
          val mPubInfo = dumpDataset.getNamedModel(pubInfoGraphName)
          val label = mPubInfo.listStatements(npUri, RDFS.label, null).asScala.toSeq
            .map(_.getObject.asLiteral().getString).headOption.getOrElse(npUri.getURI)
          benchmarkNodeToNanopubInfo(st.getObject.asResource().getURI) = (npUri, pubInfoGraphName, label)
        })
        m.listStatements(null, RdfUtil.usesTask, null).asScala
          .flatMap(st => PurlMaker.unMake(st.getResource.getURI).map(purl => (st, purl)))
          .filter((_, purl) => {
            if purl.kind != "tasks" || purl.id != taskName then
              false
            else
              val taskVersionInNanopub = Version.tryParse(purl.version)
              if taskVersionInNanopub.isEmpty then
                println(s"Warning: Task $taskName has a benchmark result with an invalid version: $purl")
                false
              // Only include benchmark results that are for the same version or older
              else taskVersionInNanopub.get.isLessOrEqualThan(taskVersion)
          })
          .flatMap((st, _) => {
            val subject = st.getSubject
            m.listSubjectsWithProperty(RdfUtil.iraoHasFollowedProtocol, subject)
              .asScala.take(1)
          })
      }).toSeq

      val resultMds = for rootNode <- benchmarkNodes yield
        val (npUri, pubInfoUri, label) = benchmarkNodeToNanopubInfo(rootNode.getURI)
        val m = ModelFactory.createDefaultModel()
        m.add(rootNode.getModel)
        val newRootNode = m.createResource()
        RdfUtil.renameResource(rootNode, newRootNode, m)
        val toAdd = dumpDataset.getNamedModel(pubInfoUri)
          .listStatements().asScala
          .filter(st => allowedPubInfoPredicates.contains(st.getPredicate))
          .map(st => m.createStatement(newRootNode, st.getPredicate, st.getObject))
          .toArray
        m.add(toAdd)
        // Rename all internal IRIs to new blank nodes for doc generation purposes
        val prefix = rootNode.getNameSpace
        val toRename = m.listSubjects().asScala.filter(_.getNameSpace == prefix).toSeq
        toRename.foreach(res => RdfUtil.renameResource(res, m.createResource(), m))
        val npId = npUri.getURI.split('/').last
        if !preloadBibFuture.isCompleted then
          println("Waiting for bibliography preloading...")
        Await.ready(preloadBibFuture, 60.seconds)
        val benchResultsDoc = benchResultsDocBuilder.build(
          label,
          s"""<span id="$npId"></span>
             |
             |!!! info
             |
             |    :fontawesome-solid-diagram-project: This benchmark result was reported in a Nanopublication: [$npUri]($npUri).
             |
             |    The documentation here was generated automatically.
             |
             |""".stripMargin,
          newRootNode
        )
        // Remove the section title
        benchResultsDoc.toMarkdown.replace("#### General information\n", "")

      val targetDir = outDir.resolve(f"tasks/$taskName")
      targetDir.toFile.mkdirs()
      Files.writeString(
        targetDir.resolve("index.md"),
        f"""${MarkdownUtil.makeTopButtons(taskPurl, fileDepth = 2)}
           |
           |# $title
           |
           |Task identifier: `$taskName`
           |
           |$description
           |
           |""".stripMargin + taskDoc.toMarkdown
      )
      val resultsBody = if resultMds.isEmpty then "_No benchmark results were reported yet for this task._"
      else resultMds.mkString("\n\n")
      Files.writeString(
        targetDir.resolve("results.md"),
        s"""${MarkdownUtil.makeTopButtons(taskPurl.copy(subpage = Some("results")), fileDepth = 2)}
           |
           |# Benchmark results for task $taskName
           |
           |[:octicons-arrow-left-24: Back to task definition](index.md)
           |
           |<div style="text-align: center" markdown>[:material-star-plus: Report your benchmark results](../../documentation/reporting-results.md){ .md-button }</div>
           |
           |$resultsBody""".stripMargin
      )

      DocFileUtil.copyDocs(f.toPath, targetDir, Seq("index.md"))
      (taskName, taskM)

    repoDir.resolve("tasks").toFile.listFiles()
      .filter(_.isDirectory)
      .map(genOne)
      .toSeq

  private def taskOverviewDocGen(version: String, tasks: Seq[(String, Model)], outDir: Path): Unit =
    val sb = new StringBuilder()
    sb.append("Task name | Identifier | Description\n")
    sb.append("--- | --- | ---\n")
    for (taskId, taskM) <- tasks.sortBy(_._1) do
      val taskRes = taskM.listSubjectsWithProperty(RDF.`type`, RdfUtil.Task).next.asResource
      val taskName = RdfUtil.getString(taskRes, RdfUtil.dctermsTitle) getOrElse taskId
      val taskDesc = (RdfUtil.getString(taskRes, RdfUtil.dctermsDescription) getOrElse "").replace('\n', ' ')
      val taskPurl = PurlMaker.task(taskId, version)
      sb.append(f"[$taskName]($taskPurl) | [`$taskId`]($taskPurl) | $taskDesc \n")

    Files.writeString(outDir.resolve("category/task_table.md"), sb.toString())

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
      val profilePurl = PurlMaker.Purl(name, version, "profiles")
      val profileDoc = profileDocBuilder.build(
        s"Profile: $name (${readableVersion(version)})",
        description + rdfInfo(profilePurl),
        profileRes
      )
      val tableSection =
        """
          |## Download links
          |
          |Below you will find links to download this profile's datasets in different lengths. The length of the dataset
          |is measured in stream elements (individual graphs or datasets) and is indicated in the table.
          |To see the size in statements (triples or quads), hover your mouse over the download link.
          |
          |!!! warning
          |
          |    Some datasets are shorter than others and a given fixed-size distribution may not be available for all datasets.
          |
          |""".stripMargin +
          Files.readString(metadataOutDir.resolve(f"profiles/doc/${name}_table.md"))
      val profileDocPath = outDir.resolve(s"profiles/$name.md")

      Files.writeString(
        profileDocPath,
        MarkdownUtil.makeTopButtons(profilePurl, fileDepth = 1) + "\n\n" + profileDoc.toMarkdown + tableSection
      )

  private def profileOverviewDocGen(version: String, profileCollection: ProfileCollection, outDir: Path): Unit =
    def taxoLink(text: String, name: String) =
      f"[$text](${Constants.taxonomyDocBaseLink}$name-stream)"

    val sb = new StringBuilder()
    sb.append("Profile | Stream type | RDF-star | Non-standard extensions\n")
    sb.append("--- | --- | :-: | :-:\n")
    for pName <- profileCollection.profiles.keys.toSeq.sorted do
      val nameSplit = pName.split('-')
      sb.append(f"[`$pName`](${PurlMaker.profile(pName, version)}) | ")
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

    Files.writeString(outDir.resolve("category/profile_table.md"), sb.toString())

  private def readableVersion(v: String) =
    if v == "dev" then "development version" else v

  private def rdfInfo(purl: PurlMaker.Purl): String =
    val baseLink = purl.getUrl
    val repo = "category-" + purl.id.split('-').head
    f"""
       |
       |!!! info
       |
       |    :fontawesome-solid-diagram-project: Download this metadata in RDF: ${MarkdownUtil.formatMetadataLinks(baseLink)}
       |    <br>:material-github: Source repository: **[$repo](${Constants.baseRepoUrl}/$repo)**
       |    <br>${MarkdownUtil.formatPurlLink(baseLink)}
       |
       |""".stripMargin