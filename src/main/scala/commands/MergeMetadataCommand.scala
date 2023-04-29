package io.github.riverbench.ci_worker
package commands

import util.*

import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object MergeMetadataCommand extends Command:
  override def name: String = "merge-metadata"

  override def description = "Merges auto-generated and manual dataset metadata and adds version-dependent info.\n" +
    "Args: <repo-dir> <package-dir> <output-dir> <version-tag>"

  override def validateArgs(args: Array[String]) = args.length == 5

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val packageDir = FileSystems.getDefault.getPath(args(2))
    val outputDir = FileSystems.getDefault.getPath(args(3))
    val versionTag = args(4)

    val mi = MetadataReader.read(repoDir)
    val (merged, datasetRes) = getMergedMetadata(repoDir, packageDir, mi, versionTag)
    addVersionMetadata(merged, datasetRes, versionTag)

    for (ext, format) <- Constants.outputFormats do
      val outFile = outputDir.resolve(f"metadata.$ext").toFile
      val os = new FileOutputStream(outFile)
      RDFDataMgr.write(os, merged, format)
      println("Wrote metadata to " + outFile.getAbsolutePath)
  }

  private def getMergedMetadata(repoDir: Path, packageDir: Path, mi: MetadataInfo, version: String):
  (Model, Resource) =
    val repoMetadata = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val packageMetadata = RDFDataMgr.loadModel(packageDir.resolve("package_metadata.ttl").toString)

    val tempDatasetRes = repoMetadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    val newDatasetRes = repoMetadata.createResource(AppConfig.CiWorker.baseDatasetUrl + mi.identifier + "/" + version)
    repoMetadata.add(packageMetadata)
    RdfUtil.renameResource(RdfUtil.tempDataset, newDatasetRes, repoMetadata)
    RdfUtil.renameResource(tempDatasetRes, newDatasetRes, repoMetadata)

    repoMetadata.removeNsPrefix("")

    (repoMetadata, newDatasetRes)

  private def addVersionMetadata(m: Model, datasetRes: Resource, version: String): Unit =
    datasetRes.addProperty(RdfUtil.hasVersion, version)
    val baseUrl = datasetRes.getURI
    datasetRes.addProperty(RdfUtil.dcatLandingPage, m.createResource(baseUrl))

    for distRes <- datasetRes.listProperties(RdfUtil.dcatDistribution).asScala.map(_.getObject.asResource).toSeq do
      val fileName = distRes.getProperty(RdfUtil.hasFileName).getObject.asLiteral.getString
      val downloadUrl = baseUrl + "/files/" + fileName
      distRes.addProperty(RdfUtil.dcatDownloadURL, m.createResource(downloadUrl))
      val distId = distRes.getProperty(RdfUtil.dctermsIdentifier).getObject.asLiteral.getString
      println(distId)
      val newDistRes = m.createResource(datasetRes.getURI + "#" + distId)
      RdfUtil.renameResource(distRes, newDistRes, m)
