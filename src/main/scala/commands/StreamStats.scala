package io.github.riverbench.ci_worker
package commands

import akka.stream.{IOResult, SubstreamCancelStrategy}
import akka.{Done, NotUsed}
import akka.stream.scaladsl.*
import util.{ArchiveReader, Constants, MetadataInfo, MetadataReader, StatCounterSuite}

import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.FileSystems
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

object StreamStats extends Command:
  override def name: String = "stream-stats"

  override def description = "..."

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val dataFile = ArchiveReader.findDataFile(repoDir)
    val metadata = MetadataReader.read(repoDir)
    val stats = new StatCounterSuite(metadata.elementCount)

    val statFuture = ArchiveReader.read(dataFile)
      .mapAsync(1)((name, byteStream) => {
        byteStream
          .runFold(String())((acc, bs) => acc + bs.utf8String)
          .map(data => (name, data))
      })
      .async
      .map((name, data) => {
        val lang = if name.endsWith(".ttl") then Lang.TTL else Lang.TRIG
        val ds = DatasetGraphFactory.create()
        RDFParser.create()
          .fromString(data)
          .lang(lang)
          .parse(ds)
        ds
      })
      .zipWithIndex
      .grouped(25)
      .splitAfter(SubstreamCancelStrategy.propagate)(datasets =>
        Constants.packageSizes.contains(datasets.last._2 + 1)
      )
      .mapAsync(3) { datasets => Future {
        for (ds, num) <- datasets do
          if !checkStructure(metadata, ds) then
            throw new Exception(s"File $num is in invalid format with relation to the metadata " +
              s"(declared element type: ${metadata.elementType}")
          stats.add(ds)
        datasets.last._2 + 1
      } }
      .reduce((a, b) => a.max(b))
      .map(num => (num, stats.result))
      .concatSubstreams
      .runWith(Sink.seq)

    Await.result(statFuture, scala.concurrent.duration.Duration.Inf)
      .foreach((num, stats) => {
        val m = ModelFactory.createDefaultModel()
        stats.addToRdf(m.createResource())

        println(s"Package $num")
        println("===========")
        m.write(System.out, "TURTLE")
        println()
        println()
      })


  private def checkStructure(metadata: MetadataInfo, ds: DatasetGraph): Boolean =
    if metadata.elementType == "triples" then
      // Only the default graph is allowed
      ds.listGraphNodes().asScala.toSeq.isEmpty
    else if metadata.elementType == "graphs" then
      // One named graph is allowed + the default graph
      ds.listGraphNodes().asScala.toSeq.size == 1
    else
      // In the quads format anything is allowed
      true
