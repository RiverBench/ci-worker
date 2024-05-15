package io.github.riverbench.ci_worker
package commands

import java.lang.module.ModuleDescriptor.Version
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object GenerateRedirectCommand extends Command:
  override def name: String = "generate-redirect"

  override def description: String = "Generates or updates a redirect for a dataset or schema PURL\n" +
    "Args: <doc repo dir (gh-pages branch)> <kind of redirected entity> <name of entity> <version of entity>"

  override def validateArgs(args: Array[String]): Boolean = args.length == 5

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val kind = args(2)
    val name = args(3)
    val version = args(4)

    if kind != "datasets" && kind != "schema" then
      println("The 'kind' parameter must be either 'datasets' or 'schema'.")
      throw new IllegalArgumentException()

    val redirectTemplate = Files.readString(
      Path.of(this.getClass.getClassLoader.getResource("redirect_template.html").toURI)
    )

    // Get the version of RiverBench we should point to
    val rbVersion = if version == "dev" then "dev" else
      repoDir.resolve("v").toFile.listFiles()
        .filter(_.isDirectory)
        .filter(_.getName.matches("[0-9.]+"))
        .sortBy(f => Version.parse(f.getName))
        .reverse
        .head
        .getName

    val redirectDir = repoDir.resolve(kind).resolve(name).resolve(version)
    redirectDir.toFile.mkdirs()
    Files.writeString(
      redirectDir.resolve("index.html"),
      redirectTemplate.replace("{{href}}", f"/v/$rbVersion/$kind/$name/")
    )
      
    println("Redirect added.")
  }
