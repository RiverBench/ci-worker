package io.github.riverbench.ci_worker
package commands

import util.external.{DoiBibliography, NanopublicationSparql, Nanopublications}

import org.eclipse.rdf4j.rio

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

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

    // DOI bibliography
    val f = Await.result(DoiBibliography.getBibliography("https://doi.org/10.48550/arXiv.2406.16412"), 30.seconds)
    println(f)
    DoiBibliography.saveCache()

    println("\n")

    // Nanopub SPARQL
    val npList = NanopublicationSparql.getForCategory("flat")
    println(npList)

    println("\n")

    // Nanopubs
    val np1 = Await.result(Nanopublications.getNanopublication(
      "https://w3id.org/np/RAyFZlqsYQ_w-j5cah_gI8WBIZxiVSM4ocWHD_tnyjLxs"), 30.seconds
    )
    println(np1)
    val np2 = Await.result(Nanopublications.getNanopublication(
      "https://w3id.org/np/RASD6Cd0oWfGXeaqDLrxSjQwI-QWNphjOv-YnqRktkt2Y"), 30.seconds
    )
    println(np2)
    Nanopublications.saveCache()

    println("\n\nSelf-test passed")
  }
