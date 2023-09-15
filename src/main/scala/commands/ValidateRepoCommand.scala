package io.github.riverbench.ci_worker
package commands

import util.ReleaseInfoParser.ReleaseInfo
import util.io.FileHelper
import util.{MetadataInfo, MetadataReader, ReleaseInfoParser}

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.shacl.lib.ShLib
import org.apache.jena.shacl.{ShaclValidator, Shapes}
import org.apache.pekko.stream.scaladsl.Sink

import java.nio.file.{FileSystems, Files, Path}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object ValidateRepoCommand extends Command:
  override def name = "validate-repo"

  override def description = "Validates the dataset repository.\n" +
    "Args: <repo dir> <source release info file> <path to metadata SHACL file>"

  override def validateArgs(args: Array[String]) =
    args.length == 4

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    if !Files.exists(repoDir) then
      println(s"Directory does not exist: $repoDir")
      System.exit(1)

    val sourceReleaseInfoFile = FileSystems.getDefault.getPath(args(2))
    if !Files.exists(sourceReleaseInfoFile) then
      println(s"Source release info file does not exist: $sourceReleaseInfoFile")
      System.exit(1)

    val shaclFile = FileSystems.getDefault.getPath(args(3))
    if !Files.exists(shaclFile) then
      println(s"SHACL file does not exist: $shaclFile")
      System.exit(1)

    val dirInspectionResult = validateDirectoryStructure(repoDir)
    if dirInspectionResult.nonEmpty then
      println("Directory structure is invalid:")
      dirInspectionResult.foreach(println)
      System.exit(1)

    println("Directory structure is valid.")

    val relInfo = ReleaseInfoParser.parse(sourceReleaseInfoFile)
    if relInfo.isLeft then
      println("Could not parse source release info file.")
      System.exit(1)

    val relAssets = relInfo.toOption.get.assets
    val datasetSources = Seq("graphs", "quads", "triples")
      .map(n => n + ".tar.gz")
      .map(relAssets.map(_.name).contains)
      .count(identity)

    if datasetSources != 1 then
      println(s"Exactly one dataset source must be present. There are $datasetSources.")
      System.exit(1)

    println("Dataset source is valid.")

    val (metadataErrors, metadataInfo) = validateMetadata(repoDir, shaclFile, relInfo.toOption.get)
    if metadataErrors.nonEmpty then
      println("Metadata is invalid:")
      metadataErrors.foreach(println)
      System.exit(1)

    println("Metadata is valid.")
    println("Stream element type: " + metadataInfo.elementType)
    println("Stream element count: " + metadataInfo.elementCount)
    (sourceReleaseInfoFile, metadataInfo)
  } flatMap { (relFile, metadataInfo) =>
    validatePackage(relFile, metadataInfo) map { packageErrors =>
      if packageErrors.nonEmpty then
        println("Package is invalid:")
        packageErrors.foreach(println)
        System.exit(1)

      println("Package is valid.")
    }
  }

  private def validateDirectoryStructure(repoDir: Path): Seq[String] =
    val files = Files.walk(repoDir).iterator().asScala.toSeq

    val errors = mutable.ArrayBuffer[String]()
    errors ++= files.flatMap { path => path match
      case _ if path.getFileName.toString.contains(" ") =>
        Some(s"File with spaces found: $path")
      case _ => None
    }

    errors ++= Seq("LICENSE", "README.md", "metadata.ttl",
      ".github/workflows/validate.yaml", ".github/workflows/release.yaml"
    ).map(repoDir.resolve)
      .filter(f => !files.contains(f))
      .map(f => s"Missing file: $f")

    if errors.nonEmpty then return errors.toSeq

    Files.readString(repoDir.resolve("LICENSE")) match
      case s if s.length < 150 || s.startsWith("PLACE HERE YOUR LICENSING INFORMATION") =>
        errors += "LICENSE file does not contain a valid license"
      case _ =>

    errors.toSeq

  private def validateMetadata(repoDir: Path, shaclFile: Path, relInfo: ReleaseInfo): (Seq[String], MetadataInfo) =
    val errors = mutable.ArrayBuffer[String]()

    def loadRdf(p: Path): Model = try
      RDFDataMgr.loadModel(p.toString)
    catch
      case e: Exception =>
        errors += s"Could not parse $p: ${e.getMessage}"
        ModelFactory.createDefaultModel()

    val model: Model = loadRdf(repoDir.resolve("metadata.ttl"))
    val shacl: Model = loadRdf(shaclFile)
    if errors.nonEmpty then
      return (errors.toSeq, MetadataInfo())

    val shapes = Shapes.parse(shacl)
    if shapes.numShapes() == 0 then
      errors += "No shapes found in SHACL file."
      return (errors.toSeq, MetadataInfo())

    val report = ShaclValidator.get().validate(shapes, model.getGraph)

    if !report.conforms() then
      errors += "Metadata does not conform to SHACL shapes"
      val buffer = new ByteArrayOutputStream()
      ShLib.printReport(buffer, report)
      errors ++= buffer.toString("utf-8").split("\n").map("  " + _)
      return (errors.toSeq, MetadataInfo())

    val mi = MetadataReader.read(repoDir)
    errors ++= mi.checkConsistency()

    // Check if the data file exists for the specified stream element type
    val dataFile = mi.elementType + ".tar.gz"
    if !relInfo.assets.map(_.name).contains(dataFile) then
      errors += s"Data file does not exist: $dataFile"

    (errors.toSeq, mi)

  private def validatePackage(relInfoFile: Path, metadataInfo: MetadataInfo): Future[Seq[String]] =
    val dataFileUrl = ReleaseInfoParser.getDatasetUrl(relInfoFile)
    val filesFuture = FileHelper.readArchive(dataFileUrl)
      .map((tarMeta, _) => {
        tarMeta.filePath.split('/').last
      })
      .runWith(Sink.seq)

    filesFuture.map { files =>
      if files.isEmpty then
        Seq("No files found in data archive.")
      else if files.length != metadataInfo.elementCount then
        Seq(s"Number of files in data archive (${files.length}) does not match" +
          s" the number of elements specified in metadata (${metadataInfo.elementCount}).")
      else
        val errors = files
          .map(_.split('.').head.toInt)
          .sliding(2)
          .flatMap(s => if s.head + 1 != s(1) then Some(s"Missing file: ${s.head + 1}") else None)
          .take(10).toSeq

        if errors.nonEmpty then
          errors
        else
          files.flatMap(f => {
            val extension = f.split('.').last
            if f.split('.').head.length != 10 then
              Some(s"File name does not match the expected format: $f")
            else if metadataInfo.elementType == "triples" then
              if extension != "ttl" then
                Some(s"File name does not match the specified stream element type: $f")
              else
                None
            else if extension != "trig" then
              Some(s"File name does not match the specified stream element type: $f")
            else
              None
          }).take(10)
    }

