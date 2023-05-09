package io.github.riverbench.ci_worker
package commands

import akka.stream.scaladsl.Source
import util.Rdf4jUtil
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.jena.sparql.graph.GraphFactory
import org.eclipse.rdf4j.rio
import org.eclipse.rdf4j.rio.RDFFormat

import java.io.{ByteArrayInputStream, File}
import java.nio.file.{FileSystems, Files}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object ParseTestCommand extends Command:
  def name: String = "parse-test"

  def description: String = "Tries to parse all files in a directory with Jena.\n" +
    "Args: <version> <in dir>"

  def validateArgs(args: Array[String]): Boolean = args.length == 2

  def run(args: Array[String]): Future[Unit] =
    val inDir = FileSystems.getDefault.getPath(args(1))

    Source.fromIterator(() => listDirectory(inDir.toFile).iterator)
      .mapAsync(6)(f => Future(doFile(f)))
      .run()
      .map(_ => ())

  private def listDirectory(d: File): Iterable[File] =
    d.listFiles()
      .flatMap(f => {
        if f.isDirectory then listDirectory(f)
        else f :: Nil
      })

  private def doFile(f: File): Unit =
    val s = Files.readString(f.toPath)
    try {
      val m = GraphFactory.createGraphMem()
      RDFParser.create()
        // most strict parsing settings possible
        .checking(true)
        .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
        .fromString(s)
        .lang(Lang.TTL)
        .parse(m)

      val rioErrListener = new rio.helpers.ParseErrorCollector()
      val parser = rio.Rio.createParser(RDFFormat.TURTLE)
        .setParseErrorListener(rioErrListener)
        .setRDFHandler(Rdf4jUtil.BlackHoleRdfHandler)
      parser.parse(new ByteArrayInputStream(s.getBytes()))

      val messages = rioErrListener.getFatalErrors.asScala ++
        rioErrListener.getErrors.asScala ++
        rioErrListener.getWarnings.asScala

      if messages.nonEmpty then
        throw new Exception(messages.mkString("\n  "))
    } catch {
      case e =>
        println(s"Error parsing ${f.getPath}")
        println(e.getMessage)
        println()
    }
