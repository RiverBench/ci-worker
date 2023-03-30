package io.github.riverbench.ci_worker
package commands

import akka.stream.*
import akka.{Done, NotUsed}
import akka.stream.scaladsl.*
import util.*

import akka.stream.alpakka.file.TarArchiveMetadata
import akka.util.ByteString
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParser, RDFWriter}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory}
import org.eclipse.rdf4j.model.vocabulary.XSD
import org.eclipse.rdf4j.rio

import java.io.{ByteArrayInputStream, FileOutputStream, InputStream}
import java.nio.file.{FileSystems, Path}
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

object StreamStatsCommand extends Command:
  override def name: String = "stream-stats"

  override def description = "Packages a dataset.\n" +
    "Args: <repo-dir> <output-dir>"

  override def validateArgs(args: Array[String]) = args.length == 3

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val outDir = FileSystems.getDefault.getPath(args(2))
    val dataFile = ArchiveHelper.findDataFile(repoDir)
    val metadata = MetadataReader.read(repoDir)
    val stats = new StatCounterSuite(metadata.elementCount)
    val packages = Constants.packageSizes
      .filter(_ <= metadata.elementCount)
      .map(s => (s, Constants.packageSizeToHuman(s)))
      ++ Seq((metadata.elementCount, "full"))

    val sinkStats = Sink.seq[(Long, StatCounterSuite.Result)]
    val sinkStreamPackage = packageStreamSink(metadata, outDir, packages)

    val g = RunnableGraph.fromGraph(GraphDSL.createGraph(sinkStats, sinkStreamPackage)
    ((_, _))
    { implicit builder =>
      (sStats, sStreamPackage) =>
      import GraphDSL.Implicits.*
      val in = ArchiveHelper.read(dataFile)
        .mapAsync(1)((name, byteStream) => {
          byteStream
            .runFold(String())((acc, bs) => acc + bs.utf8String)
            .map(data => (name, data))
        })
      val inBroad = builder.add(Broadcast[(TarArchiveMetadata, String)](2))
      val parseJenaBuffered = parseJenaFlow
        .zipWithIndex
        .buffer(50, OverflowStrategy.backpressure)
      val dsBroad = builder.add(Broadcast[(DatasetGraph, Long)](4))

      in ~> inBroad
      inBroad ~> checkRdf4jFlow.async ~> Sink.ignore
      inBroad ~> parseJenaBuffered.async ~> dsBroad ~> checkStructureFlow(metadata).async ~> Sink.ignore
      dsBroad ~> statsFlow(stats).async ~> sStats
      dsBroad ~> sStreamPackage
      // TODO: flat packaging
      dsBroad ~> Sink.ignore

      ClosedShape
    })

    val (statFuture, streamPackageFutures) = g.run()

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

    // TODO: run these futures in parallel
    Await.result(Future.sequence(streamPackageFutures), scala.concurrent.duration.Duration.Inf)
      .foreach(_ => {
        println("eeee.")
      })

    val statsFile = outDir.resolve("temp_stats_stream.ttl").toFile
    val os = new FileOutputStream(statsFile)

    m.write(os, "TURTLE")
    println("Done.")

  private def parseJenaFlow: Flow[(TarArchiveMetadata, String), DatasetGraph, NotUsed] =
    Flow[(TarArchiveMetadata, String)].map((tarMeta, data) => {
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

      ds
    })

  private def statsFlow(stats: StatCounterSuite):
  Flow[(DatasetGraph, Long), (Long, StatCounterSuite.Result), NotUsed] =
    Flow[(DatasetGraph, Long)]
      .splitAfter(SubstreamCancelStrategy.propagate)((_, num) =>
        val shouldSplit = Constants.packageSizes.contains(num + 1)
        if shouldSplit then println(s"Splitting stats stream at ${num + 1}")
        shouldSplit
      )
      .map((ds, num) => {
        stats.add(ds)
        num + 1
      })
      .reduce((a, b) => a.max(b))
      .map(num => (num, stats.result))
      .concatSubstreams

  private def checkRdf4jFlow: Flow[(TarArchiveMetadata, String), Unit, NotUsed] =
    Flow[(TarArchiveMetadata, String)].map((_, data) => {
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
    })

  private def packageStreamSink(metadata: MetadataInfo, outDir: Path, packages: Seq[(Long, String)]):
  Sink[(DatasetGraph, Long), Seq[Future[IOResult]]] =
    // TODO: checksum?!?!
    // TODO: size??!!
    val sinks = packages.map { case (size, name) =>
      Flow[(String, String)]
        .take(size)
        .toMat(ArchiveHelper.write(outDir.resolve(s"stream_$name.tar.gz"), size))(Keep.right)
    }
    Sink.fromGraph(GraphDSL.create(sinks) { implicit builder =>
      sinkList =>
      import GraphDSL.Implicits.*
      val writerFlow = Flow[(DatasetGraph, Long)]
        .map((ds, num) => {
          if metadata.elementType == "triples" then
            val data = RDFWriter.create()
              .lang(Lang.TTL)
              .source(ds.getDefaultGraph)
              .asString()
            (f"$num%010d.ttl", data)
          else
            val data = RDFWriter.create()
              .lang(Lang.TRIG)
              .source(ds)
              .asString()
            (f"$num%010d.trig", data)
        })
      val writerFlowG = builder.add(writerFlow)

      val writerBroad = builder.add(Broadcast[(String, String)](sinkList.size))

      writerFlowG.out ~> writerBroad
      for sink <- sinkList do
        writerBroad ~> sink

      SinkShape(writerFlowG.in)
    })

  private def checkStructureFlow(metadata: MetadataInfo) =
    Flow[(DatasetGraph, Long)].map((ds, _) => checkStructure(metadata, ds))

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
