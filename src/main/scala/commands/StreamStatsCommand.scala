package io.github.riverbench.ci_worker
package commands

import akka.stream.{IOResult, OverflowStrategy, SubstreamCancelStrategy}
import akka.{Done, NotUsed}
import akka.stream.scaladsl.*
import util.*

import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory}
import org.eclipse.rdf4j.model.vocabulary.XSD
import org.eclipse.rdf4j.rio

import java.io.{ByteArrayInputStream, FileOutputStream, InputStream}
import java.nio.file.FileSystems
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

object StreamStatsCommand extends Command:
  override def name: String = "stream-stats"

  override def description = "Computes statistics for a streamed variant of the dataset.\n" +
    "Args: <repo-dir>"

  override def validateArgs(args: Array[String]) = args.length == 2

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val dataFile = ArchiveHelper.findDataFile(repoDir)
    val metadata = MetadataReader.read(repoDir)
    val stats = new StatCounterSuite(metadata.elementCount)

    val statFuture = ArchiveHelper.read(dataFile)
      .mapAsync(1)((name, byteStream) => {
        byteStream
          .runFold(String())((acc, bs) => acc + bs.utf8String)
          .map(data => (name, data))
      })
      .async
      .map((tarMeta, data) => {
        val name = tarMeta.filePathName
        val lang = if name.endsWith(".ttl") then Lang.TTL else Lang.TRIG
        val ds = DatasetGraphFactory.create()
        // Parse the dataset in Jena
        RDFParser.create()
          // most strict parsing settings possible
          .checking(true)
          .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
          .fromString(data)
          .lang(lang)
          .parse(ds)

        // ...and in RDF4j to ensure it's valid for both
        val rioErrListener = new rio.helpers.ParseErrorCollector()
        val parser = rio.Rio.createParser(rio.RDFFormat.TRIGSTAR)
          .setParseErrorListener(rioErrListener)
          .setRDFHandler(Rdf4jUtil.BlackHoleRdfHandler)
        parser.parse(new ByteArrayInputStream(data.getBytes()))
        val messages = rioErrListener.getFatalErrors.asScala ++
          rioErrListener.getErrors.asScala ++
          rioErrListener.getWarnings.asScala

        if messages.nonEmpty then
          throw new Exception(s"File $name is not valid RDF: \n  ${messages.mkString("\n  ")}")

        ds
      })
      .zipWithIndex
      .buffer(100, OverflowStrategy.backpressure)
      .grouped(25)
      .splitAfter(SubstreamCancelStrategy.propagate)(datasets =>
        val shouldSplit = Constants.packageSizes.contains(datasets.last._2 + 1)
        if shouldSplit then println(s"Splitting stream at ${datasets.last._2 + 1}")
        shouldSplit
      )
      .mapAsync(3) { datasets => Future {
        for (ds, num) <- datasets do
          checkStructure(metadata, ds) match
            case Some(msg) => throw new Exception(s"File $num is not valid: $msg")
            case None => ()
          stats.add(ds)
        datasets.last._2 + 1
      } }
      .reduce((a, b) => a.max(b))
      .map(num => (num, stats.result))
      .concatSubstreams
      .runWith(Sink.seq)

    val m = ModelFactory.createDefaultModel()
    m.setNsPrefix("rb", RdfUtil.pRb)
    m.setNsPrefix("dcat", RdfUtil.pDcat)
    m.setNsPrefix("xsd", XSD.NAMESPACE)
    val datasetRes = m.createResource(RdfUtil.pTemp + "dataset")

    Await.result(statFuture, scala.concurrent.duration.Duration.Inf)
      .foreach((num, stats) => {
        val distRes = m.createResource()
        datasetRes.addProperty(RdfUtil.dcatDistribution, distRes)
        stats.addToRdf(distRes, metadata, num, false)
      })

    val statsFile = repoDir.resolve("temp_stats_stream.ttl").toFile
    val os = new FileOutputStream(statsFile)

    m.write(os, "TURTLE")
    println("Done.")

  private def checkStructure(metadata: MetadataInfo, ds: DatasetGraph): Option[String] =
    if metadata.elementType == "triples" then
      // Only the default graph is allowed
      if ds.listGraphNodes().asScala.toSeq.nonEmpty then
        return Some("There are named graphs in a triples dataset")
    else if metadata.elementType == "graphs" then
      // One named graph is allowed + the default graph
      if ds.listGraphNodes().asScala.toSeq.size != 1 then
        return Some("There must be exactly one named graph in a graphs dataset")

    if !metadata.conformance.usesGeneralizedRdfDatasets &&
      ds.listGraphNodes().asScala.exists(n => n.isLiteral) then
      return Some("The dataset contains a graph node that is a literal, " +
        "but the metadata does not declare the use of generalized RDF datasets")

    if !metadata.conformance.usesGeneralizedTriples then
      val generalizedTriples = ds.find().asScala
        .flatMap(q => q.getSubject.isLiteral :: (q.getPredicate.isLiteral || q.getPredicate.isBlank) :: Nil)
        .exists(identity)
      if generalizedTriples then
        return Some(s"The dataset contains a generalized triple, " +
          s"but the metadata does not declare the use of generalized triples.")

    if !metadata.conformance.usesRdfStar then
      val rdfStar = ds.find().asScala
        .flatMap(q => q.getGraph :: q.getSubject :: q.getPredicate :: q.getObject :: Nil)
        .exists(_.isNodeTriple)
      if rdfStar then
        return Some(s"The dataset contains an RDF-star node, " +
          s"but the metadata does not declare the use of RDF-star.")

    None
