package io.github.riverbench.ci_worker
package commands

import org.eclipse.rdf4j.rio

import scala.concurrent.Future

object SelfTestCommand extends Command:
  def description: String = "Runs a self-test to ensure the worker is working correctly"

  def name: String = "self-test"

  def validateArgs(args: Array[String]): Boolean = args.length == 1

  def run(args: Array[String]): Future[Unit] = Future {
    val rioFormats = Seq(
      rio.RDFFormat.TURTLE, rio.RDFFormat.TRIG, rio.RDFFormat.TURTLESTAR, rio.RDFFormat.TRIGSTAR,
    )
    for format <- rioFormats do
      rio.Rio.createParser(format)

    println("Self-test passed")
  }
