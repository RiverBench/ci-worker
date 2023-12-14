package io.github.riverbench.ci_worker
package commands

import util.*

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.shacl.lib.ShLib
import org.apache.jena.shacl.{ShaclValidator, Shapes}

import java.nio.file.{FileSystems, Path}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object ValidateCategoryCommand extends Command:
  override def name = "validate-category"

  override def description = "Validates the category repository.\n" +
    "Args: <repo dir> <schema repo dir>"

  override def validateArgs(args: Array[String]) =
    args.length == 3

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val schemaDir = FileSystems.getDefault.getPath(args(2))
    val shaclDir = schemaDir.resolve("src/shacl")

    val profileFiles = repoDir.resolve("profiles").toFile.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".ttl"))

    def noCheck(m: Model) = Seq()

    val validations: Seq[(Seq[Path], Path, Model => Seq[String])] =
      // Validate individual profiles
      profileFiles.map(f => (
        Seq(f.toPath),
        shaclDir.resolve("profile.ttl"),
        // Check if the ID is consistent with the file name
        (m: Model) => {
          val expectedId = f.getName.replace(".ttl", "")
          val pRes = m.createResource(AppConfig.CiWorker.baseProfileUrl + expectedId)
          if pRes.listProperties().asScala.isEmpty then
            Seq(s"Profile $pRes has no properties")
          else
            RdfUtil.getString(pRes, RdfUtil.dctermsIdentifier) match
              case Some(v) if v == expectedId => Seq()
              case Some(v) => Seq(s"Profile $pRes has wrong dcterms:identifier: $v")
              case None => Seq(s"Profile $pRes has no dcterms:identifier")
        }
      )).toSeq ++
      // Validate profile collection
      Seq((
        profileFiles.map(_.toPath).toSeq,
        shaclDir.resolve("profile-collection.ttl"),
        noCheck
      )) ++
      // Validate the tasks
      repoDir.resolve("tasks").toFile.listFiles()
        .filter(f => f.isDirectory && !f.getName.startsWith("."))
        .map(taskDir => (
          Seq(taskDir.toPath.resolve("metadata.ttl")),
          shaclDir.resolve("task.ttl"),
          // Check if the ID is consistent with the directory name
          (m: Model) => {
            val expectedId = taskDir.getName
            val tRes = m.createResource(RdfUtil.pTemp + "task")
            RdfUtil.getString(tRes, RdfUtil.dctermsIdentifier) match
              case Some(v) if v == expectedId => Seq()
              case Some(v) => Seq(s"Task $expectedId has wrong dcterms:identifier: $v")
              case None => Seq(s"Task $expectedId has no dcterms:identifier")
          }
        )).toSeq ++
      // Validate the category
      Seq((
        Seq(repoDir.resolve("metadata.ttl")),
        shaclDir.resolve("category.ttl"),
        noCheck
      ))

    val errors = validations
      .map((p, shaclFile, validate) => {
        val (errors, model) = validateFile(p, shaclFile)
        if errors.isEmpty then
          (p, errors ++ validate(model))
        else
          (p, errors)
      })
      .filter((_, errors) => errors.nonEmpty)

    if errors.nonEmpty then
      println("Validation errors:")
      errors.foreach((p, errors) => {
        if p.size == 1 then
          println(s"  ${p.head}")
        else if p.size > 1 then
          println(s"  ${p.head.getParent}")
        errors.foreach(e => println(s"    $e"))
        println()
      })
      throw new Exception("Validation failed")
    else
      println("Validation successful")
      println(f"Validated ${validations.size}%,d graphs")
  }

  private def loadRdf(p: Path, errors: mutable.ArrayBuffer[String]): Model = try
    RDFDataMgr.loadModel(p.toString)
  catch
    case e: Exception =>
      errors += s"Could not parse $p: ${e.getMessage}"
      ModelFactory.createDefaultModel()

  private def validateFile(p: Seq[Path], shaclFile: Path): (Seq[String], Model) =
    val errors = mutable.ArrayBuffer[String]()
    val model = ModelFactory.createDefaultModel()
    p.foreach(p => model.add(loadRdf(p, errors)))
    val modelToValidate = ModelFactory.createDefaultModel().add(model)
    val shapes = Shapes.parse(loadRdf(shaclFile, errors))
    val report = ShaclValidator.get().validate(shapes, modelToValidate.getGraph)
    if !report.conforms() then
      errors += "Metadata does not conform to SHACL shapes"
      val buffer = new ByteArrayOutputStream()
      ShLib.printReport(buffer, report)
      errors ++= buffer.toString("utf-8").split("\n").map("  " + _)

    (errors.toSeq, model)
