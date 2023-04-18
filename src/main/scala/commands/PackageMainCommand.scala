package io.github.riverbench.ci_worker
package commands

import util.{AppConfig, DatasetCollection, ProfileCollection, RdfUtil}
import org.apache.jena.rdf.model.{Model, Property, RDFNode, Resource}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
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
      linkProfileAndDatasets(profileModel, res, datasetCollection)
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

    // Write to files
    println("Writing profiles...")
    outDir.resolve("profiles").toFile.mkdirs()
    for (name, profileModel) <- profileCollection.profiles do
      val outFile = outDir.resolve(f"profiles/$name.ttl").toFile
      RDFDataMgr.write(new FileOutputStream(outFile), profileModel, Lang.TURTLE)

    println("Writing main metadata...")
    val mainOutFile = outDir.resolve("metadata.ttl").toFile
    RDFDataMgr.write(new FileOutputStream(mainOutFile), mainModel, Lang.TURTLE)

    println("Done.")
  }

  private def linkProfileAndDatasets(profile: Model, profileRes: Resource, datasetCollection: DatasetCollection): Unit =
    val restrictions = profile.listObjectsOfProperty(profileRes, RdfUtil.hasRestriction).asScala
      .map(rNode => {
        val rRes = rNode.asResource()
        val props = rRes.listProperties().asScala
          .map(p => (p.getPredicate, p.getObject))
          .toSeq
        props
      })
      .toSeq

    for ((_, dsModel) <- datasetCollection.datasets) do
      val dsRes = dsModel.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
      if datasetMatchesRestrictions(dsRes, restrictions) then
        profile.add(profileRes, RdfUtil.dcatSeriesMember, dsRes)

  private def datasetMatchesRestrictions(dsRes: Resource, rs: Seq[Seq[(Property, RDFNode)]]): Boolean =
    val andMatches = for r <- rs yield
      val orMatches = for (p, o) <- r yield
        if p.getURI == RdfUtil.hasDistributionType.getURI then
          None
        else if !dsRes.hasProperty(p, o) then
          Some(false)
        else Some(true)

      val orMatchesFlat = orMatches.flatten
      orMatchesFlat.isEmpty || orMatchesFlat.contains(true)
    !andMatches.contains(false)
