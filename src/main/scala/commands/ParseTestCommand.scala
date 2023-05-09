package io.github.riverbench.ci_worker
package commands

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.jena.sparql.graph.GraphFactory

import java.io.File
import java.nio.file.FileSystems
import scala.concurrent.Future

object ParseTestCommand extends Command:
  def name: String = "parse-test"

  def description: String = "Tries to parse all files in a directory with Jena.\n" +
    "Args: <version> <in dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 2

  def run(args: Array[String]): Future[Unit] = Future {
    val inDir = FileSystems.getDefault.getPath(args(1))

    doDirectory(inDir.toFile)
  }

  private def doDirectory(d: File): Unit =
    d.listFiles()
      .foreach(f => {
        if f.isDirectory then doDirectory(f)
        else doFile(f)
      })

  private def doFile(f: File): Unit =
    val is = java.nio.file.Files.newInputStream(f.toPath)
    try {
      val m = GraphFactory.createGraphMem()
      RDFParser.create()
        // most strict parsing settings possible
        .checking(true)
        .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
        .source(is)
        .lang(Lang.TTL)
        .parse(m)
    } catch {
      case e =>
        println(s"Error parsing ${f.getPath}")
        println(e.getMessage)
        println()
    } finally {
      is.close()
    }
