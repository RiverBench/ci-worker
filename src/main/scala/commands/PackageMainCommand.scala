package io.github.riverbench.ci_worker
package commands

import util.*
import util.collection.*
import util.doc.MarkdownUtil

import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, Property, RDFNode, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Files, Path}
import java.util.zip.GZIPOutputStream
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PackageMainCommand extends Command:
  override def name: String = "package-main"

  override def description: String = "Package main repo metadata, incl. profiles.\n" +
    "Args: <version> <main-repo-dir> <out-dir>"

  override def validateArgs(args: Array[String]) = args.length == 4

  override def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))

    val datasetsVersion = if version == "dev" then "dev" else "latest"

    println("Fetching categories...")
    val categoryNames = repoDir.resolve("categories").toFile.listFiles()
      .filter(_.isDirectory)
      .map(_.getName)
    val categoryCollection = CategoryCollection.fromReleases(categoryNames, datasetsVersion)

    println("Fetching datasets...")
    val datasetNames = repoDir.resolve("datasets").toFile.listFiles()
      .filter(_.isDirectory)
      .map(_.getName)
    val datasetCollection = DatasetCollection.fromReleases(datasetNames, datasetsVersion)

    println("Fetching category metadata dumps...")
    val categoryDumps = categoryCollection.categoryDumps.values

    // Prepare main model
    val mainModel = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val oldMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl)
    val mainVer = if version == "dev" then "" else "v/" + version
    val newMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl + mainVer)

    println("Processing main metadata...")
    RdfUtil.renameResource(oldMainRes, newMainRes, mainModel)
    newMainRes.addProperty(RdfUtil.foafHomepage, newMainRes)
    newMainRes.addProperty(RdfUtil.dcatVersion, version)
    // TODO: remove in 2.3.0
    newMainRes.addProperty(RdfUtil.hasVersion, version)

    // Add links to datasets, categories, profiles, and tasks
    for ((_, dsModel) <- datasetCollection.datasets) do
      val dsRes = dsModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
      newMainRes.addProperty(RdfUtil.dcatDataset, dsRes)
      // inverse property (DCAT 3)
      dsRes.addProperty(RdfUtil.dcatInCatalog, newMainRes)
    for ((_, catModel) <- categoryCollection.categories) do
      val catRes = catModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Category).next.asResource
      newMainRes.addProperty(RdfUtil.hasCategory, catRes)
      newMainRes.addProperty(RdfUtil.dcatResource, catRes)
    for catDump <- categoryDumps do
      val catModel = catDump.getDefaultModel
      val profilesAndTasks = catModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Profile).asScala
        .concat(catModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Task).asScala)
      for res <- profilesAndTasks do
        newMainRes.addProperty(RdfUtil.dcatResource, res)

    // Generate RDF dump of all metadata
    val allModels = Seq(mainModel) ++
      datasetCollection.datasets.values ++
      categoryDumps.map(_.getDefaultModel)

    val dumpModel = RdfUtil.mergeModels(allModels)
    // Final touch: add inverse properties from datasets to their profiles
    for s <- dumpModel.listStatements(null, RdfUtil.dcatSeriesMember, null).asScala do
      val dataset = s.getObject.asResource
      val profile = s.getSubject
      dataset.addProperty(RdfUtil.dcatInSeries, profile)

    val dumpWithResultsDataset = DatasetFactory.create(dumpModel)
    for categoryDump <- categoryDumps do
      for graphName <- categoryDump.listNames().asScala do
        val graph = categoryDump.getNamedModel(graphName)
        dumpWithResultsDataset.addNamedModel(graphName, graph)
    
    // Generate dataset overview
    println("Generating dataset overview...")
    generateDatasetOverview(datasetCollection, outDir)

    // Generate benchmark result summary
    println("Generating benchmark result summary...")
    generateBenchmarkResultSummary(dumpWithResultsDataset, categoryCollection, outDir, version)

    // Write to files
    println("Writing main metadata...")
    for (ext, format) <- Constants.outputFormats do
      val mainOutFile = outDir.resolve(f"metadata.$ext").toFile
      RDFDataMgr.write(new FileOutputStream(mainOutFile), mainModel, format)

    println("Writing dump...")
    for (ext, format) <- Constants.outputFormats do
      val dumpOutFile = outDir.resolve(f"dump.$ext.gz").toFile
      val os = new GZIPOutputStream(new FileOutputStream(dumpOutFile))
      RDFDataMgr.write(os, dumpModel, format)
      os.close()

    println("Writing dump with benchmark results...")
    for (ext, format) <- Constants.datasetOutputFormats do
      val dumpWithNanopubsOutFile = outDir.resolve(f"dump-with-results.$ext.gz").toFile
      val os = new GZIPOutputStream(new FileOutputStream(dumpWithNanopubsOutFile))
      RDFDataMgr.write(os, dumpWithResultsDataset, format)
      os.close()

    println("Done.")
  }

  private def generateDatasetOverview(datasetCollection: DatasetCollection, outDir: Path): Unit =
    val sb = new StringBuilder()
    sb.append("Dataset | <abbr title=\"Stream type\">El. type</abbr> | " +
      "<abbr title=\"Stream element count\">El. count</abbr> | " +
      "<abbr title=\"Does the dataset use RDF-star?\">RDF-star</abbr> | " +
      "<abbr title=\"Does the dataset use generalized triples?\">Gen. triples</abbr> | " +
      "<abbr title=\"Does the dataset use generalized RDF datasets?\">Gen. datasets</abbr>\n")
    sb.append("--- | --- | --: | :-: | :-: | :-:\n")

    for (name, model) <- datasetCollection.datasets.toSeq.sortBy(_._1) do
      val mi = MetadataReader.fromModel(model)
      sb.append(f"[`$name`]($name/index.md) | ")
      val streamType = mi.streamTypes.filterNot(_.isFlat).head
      sb.append(f"[${streamType.readableName.replace("stream", "").trim}]" +
        f"(${Constants.taxonomyDocBaseLink}${streamType.docName}) | ")
      sb.append(f"${MarkdownUtil.formatInt(mi.elementCount.toString)}")
      for b <- Seq(
        mi.conformance.usesRdfStar,
        mi.conformance.usesGeneralizedTriples,
        mi.conformance.usesGeneralizedRdfDatasets,
      ) do
        sb.append(" | ")
        sb.append(if b then ":material-check:" else ":material-close:")
      sb.append("\n")

    outDir.resolve("doc").toFile.mkdirs()
    Files.writeString(outDir.resolve(f"doc/dataset_table.md"), sb.toString())

  private def generateBenchmarkResultSummary(
    dump: Dataset, categories: CategoryCollection, outDir: Path, version: String
  ): Unit =
    val sb = new StringBuilder()
    for (name, catUri) <- categories.namesToUris.toSeq.sortBy(_._1) do
      sb.append(f"## Benchmark results for category [`$name`](${PurlMaker.category(name, version)})\n\n")
      generateBenchmarkResultTable(sb, name, categories.categoryDumps(name), version)
      sb.append("\n\n")
    outDir.resolve("doc").toFile.mkdirs()
    Files.writeString(outDir.resolve(f"doc/benchmark_results_table.md"), sb.toString())

  private def generateBenchmarkResultTable(
    sb: StringBuilder, categoryName: String, dump: Dataset, version: String
  ): Unit =
    val allNamedGraphs = dump.listNames().asScala
      .map(name => (name, dump.getNamedModel(name)))
      .toSeq
    val nanopubs = allNamedGraphs
      .flatMap((headGraph, model) => {
        val sts = model.listStatements(null, RdfUtil.npHasAssertion, null).asScala.toSeq
        if sts.isEmpty then None
        else
          val npUrl = sts.head.getSubject.getURI
          val assertionGraph = sts.head.getResource.getURI
          val pubInfoGraph = model.listStatements(null, RdfUtil.npHasPublicationInfo, null)
            .asScala.toSeq.head.getResource.getURI
          Some(npUrl, assertionGraph, pubInfoGraph)
      })
    val benchmarks = nanopubs
      .flatMap((npUrl, assertGraph, pubInfoGraph) => {
        val mAssert = dump.getNamedModel(assertGraph)
        val sts = mAssert.listStatements(null, RdfUtil.iraoHasFollowedProtocol, null).asScala.toSeq
        if sts.isEmpty then None
        else
          val protocol = sts.head.getResource
          val profile = mAssert.listObjectsOfProperty(protocol, RdfUtil.usesProfile).asScala
            .filter(_.isResource).toSeq.headOption
            .flatMap(x => PurlMaker.unMake(x.asResource.getURI))
          val task = mAssert.listObjectsOfProperty(protocol, RdfUtil.usesTask).asScala
            .filter(_.isResource).toSeq.headOption
            .flatMap(x => PurlMaker.unMake(x.asResource.getURI))
          val mPubInfo = dump.getNamedModel(pubInfoGraph)
          val date = mPubInfo.listObjectsOfProperty(mPubInfo.createResource(npUrl), RdfUtil.dctermsCreated)
            .asScala.toSeq.headOption
            .map(_.asLiteral.getString)
          Some(npUrl, profile, task, date)
      })
      // Sort by date...
      .sortBy(_._4.getOrElse("zzz"))
      // ... and by task ID
      .sortBy(_._3.map(_.id).getOrElse("zzz"))
    if benchmarks.isEmpty then
      sb.append("_No benchmark results were reported yet for this category._\n")
      return

    sb.append("Task | Profile | Date reported | Details | Source\n")
    sb.append("--- | --- | --- | --- | ---\n")
    for (npUrl, profileOption, taskOption, dateOption) <- benchmarks do
      val profile = profileOption
        .map(p => f"[${p.id}](${PurlMaker.profile(p.id, version)}) (${p.version})")
        .getOrElse("_Unknown_")
      val task = taskOption
        .map(t => f"[${t.id}](${PurlMaker.task(t.id, version)}) (${t.version})")
        .getOrElse("_Unknown_")
      val date = dateOption
        .map(dt => dt.split('T').headOption.getOrElse(dt))
        .getOrElse("_Unknown_")
      val details = if taskOption.isDefined then
        val npId = npUrl.split('/').last
        // Link to the version of the task that we are on right now, not the one that the
        // benchmark was run on.
        val taskPurl = taskOption.map(t => PurlMaker.task(t.id, version, Some("results"))).getOrElse("")
        f"[:octicons-arrow-right-24: Details]($taskPurl#$npId)"
      else "–"
      sb.append(f"$task | $profile | $date | $details | [:fontawesome-solid-diagram-project: Source]($npUrl)\n")
