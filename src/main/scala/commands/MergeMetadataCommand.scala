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
    val (merged, datasetRes) = getMergedMetadata(repoDir, packageDir)
    addVersionMetadata(merged, datasetRes, mi, versionTag)

    val outFile = outputDir.resolve("metadata.ttl").toFile
    val os = new FileOutputStream(outFile)
    RDFDataMgr.write(os, merged, RDFFormat.TURTLE_PRETTY)
    println("Wrote metadata to " + outFile.getAbsolutePath)
  }

  private def getMergedMetadata(repoDir: Path, packageDir: Path): (Model, Resource) =
    val repoMetadata = RDFDataMgr.loadModel(repoDir.resolve("metadata.ttl").toString)
    val packageMetadata = RDFDataMgr.loadModel(packageDir.resolve("package_metadata.ttl").toString)

    val datasetRes = repoMetadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    val tempStatements = packageMetadata.listStatements(RdfUtil.tempDataset, null, null).asScala.toSeq
    for tempS <- tempStatements do
      packageMetadata.add(datasetRes, tempS.getPredicate, tempS.getObject)
      packageMetadata.remove(tempS)

    repoMetadata.add(packageMetadata)
    (repoMetadata, datasetRes)

  private def addVersionMetadata(m: Model, datasetRes: Resource, mi: MetadataInfo, version: String): Unit =
    datasetRes.addProperty(RdfUtil.dcatVersion, version)
    val baseUrl = AppConfig.CiWorker.baseDownloadUrl + mi.identifier + "/" + version
    datasetRes.addProperty(RdfUtil.dcatLandingPage, m.createResource(baseUrl))

    for distRes <- datasetRes.listProperties(RdfUtil.dcatDistribution).asScala.map(_.getObject.asResource).toSeq do
      val fileName = distRes.getProperty(RdfUtil.hasFileName).getObject.asLiteral.getString
      val downloadUrl = baseUrl + "/" + fileName
      distRes.addProperty(RdfUtil.dcatDownloadURL, m.createResource(downloadUrl))
