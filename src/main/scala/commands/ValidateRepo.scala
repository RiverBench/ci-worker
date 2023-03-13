package io.github.riverbench.ci_worker
package commands

import java.nio.file.{FileSystems, Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object ValidateRepo extends Command:
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

    // TODO: check for .github
    errors ++= Seq("LICENSE", "README.md", "metadata.ttl")
      .map(repoDir.resolve)
      .filter(f => !files.contains(f))
      .map(f => s"Missing file: $f")

    errors.toSeq
