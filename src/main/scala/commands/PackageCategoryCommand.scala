package io.github.riverbench.ci_worker
package commands

import util.doc.MarkdownUtil
import util.*

import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.shacl.{ShaclValidator, Shapes}
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Files, Path}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PackageCategoryCommand extends Command:

  override def name: String = "package-category"

  override def description: String = "Package a benchmark category.\n" +
    "Args: <version> <repo-dir> <main-repo-dir> <schema-repo-dir> <out-dir>"

  override def validateArgs(args: Array[String]) = args.length == 6

  override def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val mainRepoDir = FileSystems.getDefault.getPath(args(3))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(4))
    val outDir = FileSystems.getDefault.getPath(args(5))

    packageProfiles(version, repoDir, mainRepoDir, schemaRepoDir, outDir)
  }

  private def packageProfiles(version: String, repoDir: Path, mainRepoDir: Path, schemaRepoDir: Path, outDir: Path):
  Unit =
    val mainVer = if version == "dev" then "" else "v/" + version
    val mainRes = RdfUtil.m.createResource(AppConfig.CiWorker.rbRootUrl + mainVer)

    // First, load all SHACL libs
    println("Loading SHACL libs...")
    val libs = schemaRepoDir.resolve("src/lib/profiles").toFile.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".ttl"))
      .map { file =>
        val name = file.getName.replace(".ttl", "")
        val m = RDFDataMgr.loadModel(file.getAbsolutePath)
        (name, m)
      }.toMap

    println("Loading profiles...")
    val profileCollection = new ProfileCollection(repoDir.resolve("profiles"))

    println("Fetching datasets...")
    val datasetNames = mainRepoDir.resolve("datasets").toFile.listFiles()
      .filter(_.isDirectory)
      .map(_.getName)
    val datasetsVersion = if version == "dev" then "dev" else "latest"
    val datasetCollection = DatasetCollection.fromReleases(datasetNames, datasetsVersion)

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
      // Import SHACL libs
      importShaclLibs(name, profileModel, libs)
      // Add inferred properties
      val res = subSupModel.createResource(getProfileUri(name))
      profileModel.removeAll(res, RdfUtil.isSupersetOfProfile, null)
      profileModel.add(subSupModel.listStatements(res, null, null))
      // Version metadata
      profileModel.add(res, RdfUtil.hasVersion, version)
      profileModel.add(res, RdfUtil.dcatInCatalog, mainRes)
      // Link datasets to profiles
      linkProfileAndDatasets(name, profileModel, res, datasetCollection, outDir)
      // Prettify
      profileModel.removeNsPrefix("")

    // Write to files
    println("Writing profiles...")
    for (name, profileModel) <- profileCollection.profiles do
      for (ext, format) <- Constants.outputFormats do
        val outFile = outDir.resolve(f"profiles/$name.$ext").toFile
        RDFDataMgr.write(new FileOutputStream(outFile), profileModel, format)

  private def importShaclLibs(profileName: String, profileModel: Model, libs: Map[String, Model]): Unit =
    profileModel.listObjects().asScala
      .filter(n => n.isURIResource && n.asResource().getURI.startsWith(RdfUtil.pLib))
      .foreach(libRes => {
        val libName = libRes.asResource().getURI.replace(RdfUtil.pLib, "")
        if libs.contains(libName) then
          profileModel.add(libs(libName))
          RdfUtil.renameResource(libRes.asResource, profileModel.createResource(), profileModel)
        else
          println(s"Error: profile $profileName -- SHACL lib $libName not found")
          throw new Exception(s"SHACL lib $libName not found")
      })

  private def linkProfileAndDatasets(
    name: String, profile: Model, profileRes: Resource, datasetCollection: DatasetCollection, outDir: Path
  ): Unit =
    val dsShapes = getShapes(profile, profileRes, RdfUtil.hasDatasetShape)
    val distShapes = getShapes(profile, profileRes, RdfUtil.hasDistributionShape)

    if dsShapes.isEmpty then
      throw new Exception(s"No dataset shape specified in profile $name")

    if distShapes.isEmpty then
      throw new Exception(s"No distribution shape specified in profile $name")

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
          if resMatchesRestrictions(dsRes, dsShapes) then
            profile.add(profileRes, RdfUtil.dcatSeriesMember, dsRes)
            val distributions = dsRes.listProperties(RdfUtil.dcatDistribution).asScala
              .map(_.getObject.asResource())
              .filter(d => resMatchesRestrictions(d, distShapes))
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

  private def getShapes(m: Model, profileRes: Resource, shapeProp: Property): Shapes =
    val shapes = ModelFactory.createDefaultModel()
    m.listObjectsOfProperty(profileRes, shapeProp).asScala
      .filter(_.isResource)
      .map(_.asResource())
      .foreach(r => shapes.add(getSubjectSubgraph(r)))
    Shapes.parse(shapes)

  private def resMatchesRestrictions(res: Resource, shapes: Shapes): Boolean =
    val subgraph = getSubjectSubgraph(res)
    val report = ShaclValidator.get().validate(shapes, subgraph.getGraph)
    report.conforms()

  private def getSubjectSubgraph(res: Resource): Model =
    val m = ModelFactory.createDefaultModel()
    val queue = mutable.Queue(res)
    while queue.nonEmpty do
      val r = queue.dequeue()
      m.add(r.listProperties())
      val children = r.listProperties().asScala
        .filter(_.getObject.isAnon)
        .map(_.getObject.asResource())
      queue.appendAll(children)
    m
