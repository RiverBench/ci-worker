package io.github.riverbench.ci_worker
package commands

import util.*
import util.collection.*
import util.doc.MarkdownUtil

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

    // Prepare main model
    val mainModel = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val oldMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl)
    val mainVer = if version == "dev" then "" else "v/" + version
    val newMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl + mainVer)

    println("Processing main metadata...")
    RdfUtil.renameResource(oldMainRes, newMainRes, mainModel)
    newMainRes.addProperty(RdfUtil.foafHomepage, newMainRes)
    newMainRes.addProperty(RdfUtil.hasVersion, version)

    // Add links to datasets and categories
    for ((_, dsModel) <- datasetCollection.datasets) do
      val dsRes = dsModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
      newMainRes.addProperty(RdfUtil.dcatDataset, dsRes)
    for ((_, catModel) <- categoryCollection.categories) do
      val catRes = catModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Category).next.asResource
      newMainRes.addProperty(RdfUtil.hasCategory, catRes)

    // Generate RDF dump of all metadata
    val allModels = Seq(mainModel) ++
      datasetCollection.datasets.values
    // TODO: dump should include categories, tasks, profiles...
    //  This stuff must start in the category repos.
    val dumpModel = RdfUtil.mergeModels(allModels)

    if version == "dev" then
      // Generate dataset overview
      println("Generating dataset overview...")
      generateDatasetOverview(datasetCollection, outDir)

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
      sb.append(f"[$name]($name/dev) | ")
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
