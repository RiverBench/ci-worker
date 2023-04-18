package io.github.riverbench.ci_worker
package commands

import util.{AppConfig, ProfileCollection, RdfIoUtil, RdfUtil}
import util.doc.DocBuilder

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.{RDF, RDFS}

import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object MainDocGenCommand extends Command:
  def name: String = "main-doc-gen"

  def description: String = "Generates documentation for the main repo (incl. profiles).\n" +
    "Args: <version> <main repo dir> <main metadata out dir> <schema repo dir> <output dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 6

  def run(args: Array[String]): Future[Unit] = Future {
    val version = args(1)
    val repoDir = FileSystems.getDefault.getPath(args(2))
    val mainMetadataOutDir = FileSystems.getDefault.getPath(args(3))
    val schemaRepoDir = FileSystems.getDefault.getPath(args(4))
    val outDir = FileSystems.getDefault.getPath(args(5))

    val readableVersion = version match
      case "dev" => "development version"
      case _ => version

    println("Generating profile documentation...")
    val profileCollection = new ProfileCollection(mainMetadataOutDir.resolve("profiles"))
    val ontologies = RdfIoUtil.loadOntologies(schemaRepoDir)
    profileDocGen(profileCollection, ontologies, outDir, readableVersion)

    println("Generating main documentation...")
  }

  private def profileDocGen(profileCollection: ProfileCollection, ontologies: Model, outDir: Path, version: String):
  Unit =
    val profileDocOpt = DocBuilder.Options(
      titleProps = Seq(
        RdfUtil.dctermsTitle,
        RDFS.label,
        RDF.`type`,
      ),
      hidePropsInLevel = Seq(
        (1, RdfUtil.dctermsDescription), // shown as content below the header
        (1, RDF.`type`), // Always the same
      )
    )
    val profileDocBuilder = new DocBuilder(ontologies, profileDocOpt)
    outDir.resolve("profiles").toFile.mkdirs()

    for (name, profile) <- profileCollection.profiles do
      val profileRes = profile.listSubjectsWithProperty(RDF.`type`, RdfUtil.Profile).next.asResource
      val description = RdfUtil.getString(profileRes, RdfUtil.dctermsDescription) getOrElse ""
      val profileDoc = profileDocBuilder.build(
        s"$name ($version)",
        description,
        profileRes
      )
      val profileDocPath = outDir.resolve(s"profiles/$name.md")
      Files.writeString(profileDocPath, profileDoc.toMarkdown)
