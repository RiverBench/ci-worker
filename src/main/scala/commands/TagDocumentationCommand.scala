package io.github.riverbench.ci_worker
package commands

import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

object TagDocumentationCommand extends Command:
  def name: String = "tag-docs"

  def description: String = "Copies current documentation for a tagged release.\n" +
    "Args: <source doc dir> <output doc dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 3

  private val rxLink = """]\((\.\./[^:)]+\.md\s*)\)""".r

  def run(args: Array[String]): Future[Unit] = Future {
    val inDir = FileSystems.getDefault.getPath(args(1))
    val outDir = FileSystems.getDefault.getPath(args(2))

    inDir.toFile.listFiles()
      .filter(_.isFile)
      .foreach { in =>
        val out = outDir.resolve(in.toPath.getFileName)
        copyFile(in.toPath, out)
      }

    println("Done.")
  }

  def copyFile(in: Path, out: Path): Unit =
    println(s"Copying ${in.getFileName}...")
    if !in.getFileName.toString.endsWith(".md") then
      Files.copy(in, out)
      return

    val content = Files.readString(in)
    val newContent = rxLink.replaceAllIn(
      content,
      m => f"](../../${m.group(1)})"
    )
    Files.writeString(out, newContent)
