package io.github.riverbench.ci_worker
package commands

import io.github.riverbench.ci_worker.util.{AppConfig, DatasetCollection, ProfileCollection, RdfUtil}
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future

object PackageProfilesCommand extends Command:
  override def name: String = "package-profiles"

  override def description: String = "Package profiles, inferring additional properties.\n" +
    "Args: <version> <main-repo-dir> <out-dir>"

  override def validateArgs(args: Array[String]) = args.length == 4

  override def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))

    val profileCollection = new ProfileCollection(repoDir.resolve("profiles"))
    val datasetCollection = new DatasetCollection(repoDir.resolve("datasets"))
    val subSupModel = profileCollection.getSubSuperAssertions

    for (name, profileModel) <- profileCollection.profiles do
      // Add inferred properties
      val res = subSupModel.createResource(RdfUtil.pProfile + name)
      profileModel.add(subSupModel.listStatements(res, null, null))

      // Add version tags to URIs
      val oldRes = profileModel.createResource(RdfUtil.pProfile + name)
      val newRes = profileModel.createResource(AppConfig.CiWorker.baseProfileUrl + name + "/" + version)
      RdfUtil.renameResource(oldRes, newRes, profileModel)

      linkProfileAndDatasets(profileModel, newRes, datasetCollection)

    // Write to files
    for (name, profileModel) <- profileCollection.profiles do
      val outFile = outDir.resolve(name + ".ttl").toFile
      RDFDataMgr.write(new FileOutputStream(outFile), profileModel, Lang.TURTLE)
  }

  private def linkProfileAndDatasets(profile: Model, profileRes: Resource, datasetCollection: DatasetCollection): Unit =
    for (dataset <- datasetCollection.datasets) do
      ()
