package io.github.riverbench.ci_worker
package commands

import io.github.riverbench.ci_worker.util.rdf.RdfUtil
import org.apache.jena.riot.RDFDataMgr

import java.lang.module.ModuleDescriptor.Version
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object GenerateRedirectCommand extends Command:
  override def name: String = "generate-redirect"

  override def description: String = "Generates or updates a redirect for a dataset or schema PURL\n" +
    "Args: <doc repo dir (gh-pages branch)> <kind of redirected entity> <name of entity> <version of RiverBench>"

  override def validateArgs(args: Array[String]): Boolean = args.length == 5

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val kind = args(2)
    val name = args(3)
    val rbVersion = args(4)

    if kind != "datasets" && kind != "schema" then
      println("The 'kind' parameter must be either 'datasets' or 'schema'.")
      throw new IllegalArgumentException()

    val version = if rbVersion == "dev" then "dev"
    else
      if kind == "datasets" then
        val m = RDFDataMgr.loadModel(
          s"https://github.com/RiverBench/dataset-$name/releases/latest/download/metadata.ttl"
        )
        RdfUtil.getString(null, RdfUtil.dcatVersion, Some(m)).get
      else
        val m = RDFDataMgr.loadModel(
          "https://github.com/RiverBench/schema/releases/latest/download/metadata.ttl"
        )
        m.getProperty(null, RdfUtil.owlVersionIri).getResource.getURI.split('/').last
    
    val redirectTemplate = String(
      getClass.getResourceAsStream("/redirect_template.html").readAllBytes()
    )

    val redirectDir = repoDir.resolve(kind).resolve(name).resolve(version)
    redirectDir.toFile.mkdirs()
    Files.writeString(
      redirectDir.resolve("index.html"),
      redirectTemplate.replace("{{href}}", f"/v/$rbVersion/$kind/$name/")
    )
      
    println(f"Redirect from $name $version to RiverBench $rbVersion added.")
  }
