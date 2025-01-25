package io.github.riverbench.ci_worker
package commands

import util.*
import util.rdf.*
import util.releases.PreviousVersionHelper

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.vocabulary.RDF

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
import java.time.LocalDate
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
    // Remove usage info – it's added in the automatic metadata
    RdfUtil.removePropertyValuesDeep(tempDatasetRes, RdfUtil.staxHasStreamTypeUsage, repoMetadata)

    val newDatasetRes = repoMetadata.createResource(PurlMaker.dataset(mi.identifier, version))
    repoMetadata.add(packageMetadata)
    RdfUtil.renameResource(RdfUtil.tempDataset, newDatasetRes, repoMetadata)
    RdfUtil.renameResource(tempDatasetRes, newDatasetRes, repoMetadata)

    repoMetadata.removeNsPrefix("")

    (repoMetadata, newDatasetRes)

  private def addVersionMetadata(m: Model, datasetRes: Resource, version: String): Unit =
    datasetRes.addProperty(RdfUtil.dcatVersion, version)
    // TODO: Remove in 2.3.0
    datasetRes.addProperty(RdfUtil.hasVersion, version)

    if version != "dev" then
      // Try to add property dcat:previousVersion
      val identifier = datasetRes.getProperty(RdfUtil.dctermsIdentifier).getObject.asLiteral.getString
      PreviousVersionHelper.addPreviousVersionInfoSynchronous(datasetRes, f"dataset-$identifier")

    val baseUrl = datasetRes.getURI
    datasetRes.addProperty(RdfUtil.dcatLandingPage, m.createResource(baseUrl))
    datasetRes.addProperty(RdfUtil.dctermsModified, m.createTypedLiteral(LocalDate.now.toString, XSDdate))

    for distRes <- datasetRes.listProperties(RdfUtil.dcatDistribution).asScala.map(_.getObject.asResource).toSeq do
      val fileName = distRes.getProperty(RdfUtil.hasFileName).getObject.asLiteral.getString
      val downloadUrl = baseUrl + "/files/" + fileName
      distRes.addProperty(RdfUtil.dcatDownloadURL, m.createResource(downloadUrl))
      val distId = distRes.getProperty(RdfUtil.dctermsIdentifier).getObject.asLiteral.getString
      val newDistRes = m.createResource(datasetRes.getURI + "#" + distId)
      RdfUtil.renameResource(distRes, newDistRes, m)

    for statSet <- m.listObjectsOfProperty(RdfUtil.hasStatisticsSet).asScala.distinct.toSeq do
      val newStatSet = m.createResource(datasetRes.getURI + "#" + statSet.asResource.getURI.split("#").last)
      RdfUtil.renameResource(statSet.asResource, newStatSet, m)
