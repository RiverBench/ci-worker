package io.github.riverbench.ci_worker
package commands

import util.doc.MarkdownUtil
import util.*

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

    println("Loading profiles...")
    val profileCollection = new ProfileCollection(repoDir.resolve("profiles"))

    println("Fetching datasets...")
    val datasetNames = repoDir.resolve("datasets").toFile.listFiles()
      .filter(_.isDirectory)
      .map(_.getName)
    val datasetsVersion = if version == "dev" then "dev" else "latest"
    val datasetCollection = DatasetCollection.fromReleases(datasetNames, datasetsVersion)

    // Prepare main model
    val mainModel = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val oldMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl)
    val mainVer = if version == "dev" then "" else "v/" + version
    val newMainRes = mainModel.createResource(AppConfig.CiWorker.rbRootUrl + mainVer)

    println("Processing profiles...")
    outDir.resolve("profiles/doc").toFile.mkdirs()
    val subSupModel = profileCollection.getSubSuperAssertions

    def getProfileUri(name: String) = AppConfig.CiWorker.baseProfileUrl + name + "/" + version

    for (name, profileModel) <- profileCollection.profiles do
      // Add version tags to URIs
      val oldRes = profileModel.createResource(AppConfig.CiWorker.baseProfileUrl + name)
      val newRes = profileModel.createResource(getProfileUri(name))
      RdfUtil.renameResource(oldRes, newRes, profileModel)
      RdfUtil.renameResource(oldRes, newRes, subSupModel)

    for (name, profileModel) <- profileCollection.profiles do
      // Add inferred properties
      val res = subSupModel.createResource(getProfileUri(name))
      profileModel.removeAll(res, RdfUtil.isSupersetOfProfile, null)
      profileModel.add(subSupModel.listStatements(res, null, null))
      // Version metadata
      profileModel.add(res, RdfUtil.hasVersion, version)
      profileModel.add(res, RdfUtil.dcatInCatalog, newMainRes)
      // Link datasets to profiles
      linkProfileAndDatasets(name, profileModel, res, datasetCollection, outDir)
      // Prettify
      profileModel.removeNsPrefix("")

    println("Processing main metadata...")
    RdfUtil.renameResource(oldMainRes, newMainRes, mainModel)
    newMainRes.addProperty(RdfUtil.foafHomepage, newMainRes)
    newMainRes.addProperty(RdfUtil.hasVersion, version)

    // Add links to datasets and profiles
    for (name, _) <- profileCollection.profiles do
      val profileRes = mainModel.createResource(getProfileUri(name))
      newMainRes.addProperty(RdfUtil.hasProfile, profileRes)
    for ((_, dsModel) <- datasetCollection.datasets) do
      val dsRes = dsModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
      newMainRes.addProperty(RdfUtil.dcatDataset, dsRes)

    val allModels = profileCollection.profiles.values ++
      Seq(mainModel) ++
      datasetCollection.datasets.values
    val dumpModel = RdfUtil.mergeModels(allModels.toSeq)

    if version == "dev" then
      // Generate dataset overview
      println("Generating dataset overview...")
      generateDatasetOverview(datasetCollection, outDir)

    // Write to files
    println("Writing profiles...")
    for (name, profileModel) <- profileCollection.profiles do
      for (ext, format) <- Constants.outputFormats do
        val outFile = outDir.resolve(f"profiles/$name.$ext").toFile
        RDFDataMgr.write(new FileOutputStream(outFile), profileModel, format)

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

  private def linkProfileAndDatasets(
    name: String, profile: Model, profileRes: Resource, datasetCollection: DatasetCollection, outDir: Path
  ): Unit =
    val restrictions = profile.listObjectsOfProperty(profileRes, RdfUtil.hasRestriction).asScala
      .map(rNode => {
        val rRes = rNode.asResource()
        val props = rRes.listProperties().asScala
          .map(p => (p.getPredicate, p.getObject))
          .toSeq
        props
      })
      .toSeq

    val distTypes = restrictions.flatten.filter(_._1 == RdfUtil.hasDistributionType)
      .map(_._2.asResource())

    if distTypes.isEmpty then
      throw new Exception(s"No distribution types specified in profile $name")

    val profileTableSb = StringBuilder()
    // name, dataset uri, Seq(dist download url, size, byte size)
    val datasets: mutable.ArrayBuffer[(String, Resource, Seq[(String, Long, Long)])] = mutable.ArrayBuffer()

    for ((name, dsModel) <- datasetCollection.datasets) do
      if dsModel.isEmpty then
        throw new Exception(f"Dataset $name is empty – does it have a matching release?")
      val dsRes = dsModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).asScala.toSeq.headOption
      dsRes match
        case None => throw new Exception(f"Could not find the root resource for dataset $name")
        case Some(dsRes) =>
          if datasetMatchesRestrictions(dsRes, restrictions) then
            profile.add(profileRes, RdfUtil.dcatSeriesMember, dsRes)
            val distributions = dsRes.listProperties(RdfUtil.dcatDistribution).asScala
              .map(_.getObject.asResource())
              .filter(d => distTypes.exists(dt => d.hasProperty(RdfUtil.hasDistributionType, dt)))
              .map(distRes => {
                val downloadUrl = distRes.getProperty(RdfUtil.dcatDownloadURL).getObject.asResource().getURI
                val size = distRes.getProperty(RdfUtil.hasStreamElementCount).getObject.asLiteral().getLong
                val byteSize = distRes.getProperty(RdfUtil.dcatByteSize).getObject.asLiteral().getLong
                (downloadUrl, size, byteSize)
              })
              .toSeq
              .sortBy(_._2)
            datasets.append((name, dsRes, distributions))

    val columns = datasets
      .flatMap(_._3.map(_._2))
      .map(c => {
        if Constants.packageSizes.contains(c) then
          (c, Constants.packageSizeToHuman(c))
        else
          (Long.MaxValue, "Full")
      })
      .distinct
      .sortBy(_._1)

    if name.contains("flat") then
      writeTable("")
    else
      profileTableSb.append(
        """!!! note
          |
          |    For stream profiles, there are two available types of distributions: plain streaming, and streaming in the Jelly format. See the [documentation](../../documentation/dataset-release-format.md) for details.
          |
          |### Plain streaming distributions
          |
          |""".stripMargin)
      writeTable("tar.gz")
      profileTableSb.append(
        """
          |
          |### Jelly streaming distributions
          |
          |""".stripMargin)
      writeTable("jelly.gz")

    def writeTable(filterBy: String): Unit =
      profileTableSb.append("Dataset")
      for col <- columns do
        profileTableSb.append(f" | ${col._2}")
      profileTableSb.append("\n---")
      profileTableSb.append(" | ---" * columns.size)

      for (dsName, dsUri, dists) <- datasets.sortBy(_._1) do
        profileTableSb.append(f"\n[$dsName]($dsUri)")
        for col <- columns do
          val (distUrl, distSize, distByteSize) = dists
            .filter(d => d._2 <= col._1 && d._1.contains(filterBy))
            .last
          profileTableSb.append(f" | [${Constants.packageSizeToHuman(distSize, true)} " +
            f"(${MarkdownUtil.formatSize(distByteSize)})]($distUrl)")

    Files.writeString(outDir.resolve(f"profiles/doc/${name}_table.md"), profileTableSb.toString())

  private def datasetMatchesRestrictions(dsRes: Resource, rs: Seq[Seq[(Property, RDFNode)]]): Boolean =
    val andMatches = for r <- rs yield
      val orMatches = for (p, o) <- r yield
        if p == RdfUtil.hasDistributionType then
          None
        else if p == RdfUtil.staxHasStreamType then
          Some(
            dsRes.listProperties(RdfUtil.staxHasStreamTypeUsage).asScala
              .exists(_.getResource.hasProperty(p, o))
          )
        else if !dsRes.hasProperty(p, o) then
          Some(false)
        else Some(true)

      val orMatchesFlat = orMatches.flatten
      orMatchesFlat.isEmpty || orMatchesFlat.contains(true)
    !andMatches.contains(false)

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
