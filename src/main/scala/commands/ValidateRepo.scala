package io.github.riverbench.ci_worker
package commands

import io.github.riverbench.ci_worker.util.ArchiveReader
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.shacl.lib.ShLib
import org.apache.jena.shacl.{ShaclValidator, Shapes}

import java.nio.file.{FileSystems, Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object ValidateRepo extends Command:
  case class MetadataInfo(elementType: String = "", elementCount: Int = 0)

  override def name = "validate-repo"

  override def description = "Validates the dataset repository.\n" +
    "Args: <repo dir> <path to metadata SHACL file>"

  override def validateArgs(args: Array[String]) =
    args.length == 3

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    if !Files.exists(repoDir) then
      println(s"Directory does not exist: $repoDir")
      System.exit(1)

    val shaclFile = FileSystems.getDefault.getPath(args(2))
    if !Files.exists(shaclFile) then
      println(s"SHACL file does not exist: $shaclFile")
      System.exit(1)

    val dirInspectionResult = validateDirectoryStructure(repoDir)
    if dirInspectionResult.nonEmpty then
      println("Directory structure is invalid:")
      dirInspectionResult.foreach(println)
      System.exit(1)
    else
      println("Directory structure is valid.")

    val (metadataErrors, metadataInfo) = validateMetadata(repoDir, shaclFile)
    if metadataErrors.nonEmpty then
      println("Metadata is invalid:")
      metadataErrors.foreach(println)
      System.exit(1)
    else
      println("Metadata is valid.")

  private def validateDirectoryStructure(repoDir: Path): Seq[String] =
    val files = Files.walk(repoDir).iterator().asScala.toSeq

    val errors = mutable.ArrayBuffer[String]()
    errors ++= files.flatMap { path => path match
      case _ if path.getFileName.toString.contains(" ") =>
        Some(s"File with spaces found: $path")
      case _ => None
    }

    val datasetSources = Seq("graphs", "quads", "triples")
      .map(n => "data/" + n + ".tar.gz")
      .map(repoDir.resolve)
      .map(files.contains)
      .count(identity)

    if datasetSources != 1 then
      errors += s"Exactly one dataset source must be present. There are $datasetSources."

    errors ++= Seq("LICENSE", "README.md", "metadata.ttl", ".github/workflows/ci.yaml")
      .map(repoDir.resolve)
      .filter(f => !files.contains(f))
      .map(f => s"Missing file: $f")

    errors.toSeq

  private def validateMetadata(repoDir: Path, shaclFile: Path): (Seq[String], MetadataInfo) =
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

    // Check if the data file exists for the specified stream element type
    val rb = "https://riverbench.github.io/schema/dataset#"
    val types = model.listObjectsOfProperty(model.createProperty(rb + "hasStreamElementType"))
      .asScala.toSeq

    if types.length != 1 then
      errors += s"Exactly one stream element type must be specified. There are ${types.length}."
      return (errors.toSeq, MetadataInfo())

    val streamType = types.head.asResource.getURI.split('#').last
    val dataFile = repoDir.resolve("data").resolve(streamType + ".tar.gz")
    if !Files.exists(dataFile) then
      errors += s"Data file does not exist: $dataFile"

    (errors.toSeq, MetadataInfo())

  private def validatePackage(repoDir: Path, metadataInfo: MetadataInfo): Seq[String] =
    val dataFile = ArchiveReader.findDataFile(repoDir)
    // TODO
    // we'll need to force the files in the tar to be stored in order...

    ArchiveReader.read(dataFile).map(_ => Seq.empty)
    Seq()