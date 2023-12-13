package io.github.riverbench.ci_worker
package commands

import util.{AppConfig, Constants, RdfUtil}

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.vocabulary.{OWL, OWL2, RDF}

import java.io.FileOutputStream
import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PackageSchemaCommand extends Command:
  override def name: String = "package-schema"

  override def description: String = "Packages the schema repository\n" +
    "Args: <version> <schema repo dir> <output dir> <doc output dir>"

  override def validateArgs(args: Array[String]) = args.length == 5

  override def run(args: Array[String]) = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))
    val docOutDir = FileSystems.getDefault.getPath(args(4))

    val toProcessNames = Seq("metadata", "documentation", "theme") ++
      repoDir.resolve("src/shacl").toFile.listFiles()
        .filter(f => f.isFile && f.getName.endsWith(".ttl"))
        .map(f => "shacl/" + f.getName.replace(".ttl", ""))
    val schemaBase = AppConfig.CiWorker.rbRootUrl + "schema/"

    outDir.toFile.mkdirs()

    if !validateShaclLibs(repoDir) then
      println("Aborting")
      throw new Exception()

    for name <- toProcessNames do
      val inPath = repoDir.resolve(s"src/$name.ttl")
      val model = RDFDataMgr.loadModel(inPath.toString)
      model.listSubjectsWithProperty(RDF.`type`, OWL.Ontology).asScala.toList match
        case List(ontology) =>
          // Update version IRI
          model.removeAll(ontology, OWL2.versionIRI, null)
          model.add(ontology, OWL2.versionIRI, model.createResource(s"${ontology.getURI}/$version"))

          // Update imports
          ontology.listProperties(OWL.imports).asScala
            .filter(_.getObject.isURIResource)
            .map(_.getObject.asResource)
            .filter(_.getURI.startsWith(schemaBase))
            .map(r => (r, r.getURI.drop(schemaBase.length).split('/')))
            .filter(_._2.length == 2)
            .toSeq
            .foreach { (oldRes, nameParts) =>
              val newRes = model.createResource(s"$schemaBase${nameParts(0)}/$version")
              model.remove(ontology, OWL.imports, oldRes)
              model.add(ontology, OWL.imports, newRes)
            }
        case _ => ()

      // Save
      println(s"Saving $name")
      if !name.contains("shacl") then
        prepOntologyForDoc(model, name, docOutDir)

      for (ext, format) <- Constants.outputFormats do
        val newName = if name.contains("shacl/") then name.replace("shacl/", "") + "-shacl" else name
        val outPath = outDir.resolve(s"$newName.$ext")
        RDFDataMgr.write(new FileOutputStream(outPath.toFile), model, format)
  }

  private def validateShaclLibs(repoDir: Path): Boolean =
    val badLibs = repoDir.resolve("src/lib/profiles").toFile.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".ttl"))
      .flatMap { file =>
        val name = file.getName.replace(".ttl", "")
        val m = RDFDataMgr.loadModel(file.getAbsolutePath)
        if m.contains(m.createResource(RdfUtil.pLib + name), null, null) then
          None
        else
          Some(name)
      }
    if badLibs.nonEmpty then
      println("The following SHACL libraries have mismatched URIs and filenames:")
      badLibs.foreach(println)
      false
    else
      true

  private def prepOntologyForDoc(m: Model, name: String, outDir: Path): Unit =
    if name != "theme" then
      val externalSubjects = m.listSubjects().asScala
        .filter(_.isURIResource)
        .filter(!_.getURI.startsWith(AppConfig.CiWorker.rbRootUrl))
        .map(_.listProperties().asScala.toSeq)
        .filter(sts => sts.size == 1 && sts.head.getPredicate == RDF.`type`)
        .map(sts => sts.head.getSubject.asResource)
        .toSeq

      for s <- externalSubjects do
        m.removeAll(s, null, null)

    val os = new FileOutputStream(outDir.resolve(f"$name.ttl").toFile)
    RDFDataMgr.write(os, m, RDFFormat.TURTLE)
    os.flush()
    os.close()