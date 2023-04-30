package io.github.riverbench.ci_worker
package commands

import akka.stream.*
import akka.{Done, NotUsed}
import akka.stream.scaladsl.*
import util.*
import util.io.*

import akka.stream.alpakka.file.TarArchiveMetadata
import akka.util.ByteString
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{ModelFactory, Resource}
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParser, RDFWriter}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory}
import org.eclipse.rdf4j.model.vocabulary.XSD
import org.eclipse.rdf4j.rio

import java.io.{ByteArrayInputStream, FileOutputStream, InputStream}
import java.nio.file.{FileSystems, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PackageCommand extends Command:
  private case class PartialResult(size: Long, stats: StatCounterSuite.Result, saveRes: SaveResult, flat: Boolean)

  override def name: String = "package"

  override def description = "Packages a dataset.\n" +
    "Args: <repo-dir> <source-rel-info-file> <output-dir>"

  override def validateArgs(args: Array[String]) = args.length == 4

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val sourceRelInfoFile = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))

    val dataFileUrl = ReleaseInfoParser.getDatasetUrl(sourceRelInfoFile)
    val metadata = MetadataReader.read(repoDir)
    val stats = new StatCounterSuite(metadata.elementCount)
    val packages = Constants.packageSizes
      .filter(_ <= metadata.elementCount)
      .map(s => (s, Constants.packageSizeToHuman(s)))
      ++ Seq((metadata.elementCount, "full"))

    val sinkStats = Sink.seq[(Long, StatCounterSuite.Result)]
    val sinkStreamPackage = packageStreamSink(metadata, outDir, packages)
    val sinkFlatPackage = packageFlatSink(metadata, outDir, packages)
    val sinkChecks = Sink.ignore

    val g = RunnableGraph.fromGraph(GraphDSL.createGraph(sinkStats, sinkStreamPackage, sinkFlatPackage, sinkChecks)
    ((_, _, _, _))
    { implicit builder =>
      (sStats, sStreamPackage, sFlatPackage, sChecks) =>
      import GraphDSL.Implicits.*
      val in = FileHelper.readArchive(dataFileUrl)
        .map((name, bytes) => (name, bytes.utf8String))
      val inBroad = builder.add(Broadcast[(TarArchiveMetadata, String)](2))
      val parseJenaBuffered = parseJenaFlow
        .zipWithIndex
        .buffer(50, OverflowStrategy.backpressure)
      val dsBroad = builder.add(Broadcast[(DatasetGraph, Long)](4))
      val checksMerge = builder.add(Merge[Unit](2))

      in ~> inBroad
      inBroad ~> checkRdf4jFlow(metadata) ~> checksMerge ~> sChecks
      inBroad ~> parseJenaBuffered.async ~> dsBroad ~> checkStructureFlow(metadata).async ~> checksMerge
      dsBroad ~> statsFlow(stats) ~> sStats
      dsBroad ~> sStreamPackage
      dsBroad ~> sFlatPackage

      ClosedShape
    }).mapMaterializedValue((fStats, fSeqStreamRes, fSeqFlatRes, fChecks) => {
      val fStreamRes = Future.sequence(fSeqStreamRes)
      val fFlatRes = Future.sequence(fSeqFlatRes)
      for
        stats <- fStats
        streamRes <- fStreamRes
        flatRes <- fFlatRes
        // There is no value returned from the checks sink, but we need to ensure it is run
        _ <- fChecks
      yield
        val sr = for ((num, stat), streamR) <- stats.zip(streamRes) yield
          PartialResult(num, stat, streamR, false)
        val fr = for ((num, stat), flatR) <- stats.zip(flatRes) yield
          PartialResult(num, stat, flatR, true)
        sr ++ fr
    })

    val m = ModelFactory.createDefaultModel()
    m.setNsPrefix("rb", RdfUtil.pRb)
    m.setNsPrefix("dcat", RdfUtil.pDcat)
    m.setNsPrefix("xsd", XSD.NAMESPACE)
    m.setNsPrefix("spdx", RdfUtil.pSpdx)

    val datasetRes = m.createResource(RdfUtil.tempDataset.getURI)

    (g.run(), m, datasetRes, outDir, metadata)
  } flatMap { (pResultsF, m, datasetRes, outDir, metadata) =>
    pResultsF map { pResults =>
      for pResult <- pResults do
        distributionToRdf(datasetRes, metadata, pResult)

      val statsFile = outDir.resolve("package_metadata.ttl").toFile
      val os = new FileOutputStream(statsFile)

      m.write(os, "TURTLE")
      println("Done.")
    }
  }

  /**
   * Adds RDF metadata for a given distribution
   * @param datasetRes the dataset resource
   * @param mi dataset metadata
   * @param pResult partial result for the distribution
   */
  private def distributionToRdf(datasetRes: Resource, mi: MetadataInfo, pResult: PartialResult):
  Unit =
    val m = datasetRes.getModel
    val distRes = m.createResource(RdfUtil.tempDataset.getURI + "#" + pResult.saveRes.getLocalId)
    datasetRes.addProperty(RdfUtil.dcatDistribution, distRes)
    distRes.addProperty(RdfUtil.hasFileName, pResult.saveRes.name)
    pResult.stats.addToRdf(distRes, mi, pResult.size, pResult.flat)
    pResult.saveRes.addToRdf(distRes, mi, pResult.flat)

  /**
   * Creates a flow for parsing the input stream with Jena to DatasetGraphs
   * @return a flow that parses the input stream
   */
  private def parseJenaFlow: Flow[(TarArchiveMetadata, String), DatasetGraph, NotUsed] =
    Flow[(TarArchiveMetadata, String)].map((tarMeta, data) => {
      val name = tarMeta.filePathName
      val lang = if name.endsWith(".ttl") then Lang.TTL else Lang.TRIG
      val ds = DatasetGraphFactory.create()

      // Parse the dataset in Jena
      try {
        RDFParser.create()
          // most strict parsing settings possible
          .checking(true)
          .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
          .fromString(data)
          .lang(lang)
          .parse(ds)
        ds
      } catch {
        case e =>
          println(s"Error parsing $name")
          throw e
      }
    })

  /**
   * Creates a flow for computing dataset statistics per distribution
   * @param stats the statistics object to use
   * @return a flow that computes statistics
   */
  private def statsFlow(stats: StatCounterSuite):
  Flow[(DatasetGraph, Long), (Long, StatCounterSuite.Result), NotUsed] =
    Flow[(DatasetGraph, Long)]
      .async
      .wireTap((_, num) => if (num + 1) % 100_000 == 0 then println(s"Stats stream at: ${num + 1}"))
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

  /**
   * Checks that the data can be parsed by RDF4J without errors
   * @return a flow that checks the data
   */
  private def checkRdf4jFlow(mi: MetadataInfo): Flow[(TarArchiveMetadata, String), Unit, NotUsed] =
    val format = mi.elementType match
      case "triples" =>
        if mi.conformance.usesRdfStar then rio.RDFFormat.TURTLESTAR else rio.RDFFormat.TURTLE
      case _ =>
        if mi.conformance.usesRdfStar then rio.RDFFormat.TRIGSTAR else rio.RDFFormat.TRIG

    Flow[(TarArchiveMetadata, String)]
      .async
      .map((_, data) => {
        val rioErrListener = new rio.helpers.ParseErrorCollector()
        val parser = rio.Rio.createParser(format)
          .setParseErrorListener(rioErrListener)
          .setRDFHandler(Rdf4jUtil.BlackHoleRdfHandler)
        parser.parse(new ByteArrayInputStream(data.getBytes()))
        val messages = rioErrListener.getFatalErrors.asScala ++
          rioErrListener.getErrors.asScala ++
          rioErrListener.getWarnings.asScala

        if messages.nonEmpty then
          throw new Exception(s"File $name is not valid RDF: \n  ${messages.mkString("\n  ")}")
      })

  /**
   * Creates a sink that writes the data as packaged streams
   * @param metadata the metadata of the dataset
   * @param outDir the directory to write the files to
   * @param packages the packages to create (size, name)
   * @return the sink
   */
  private def packageStreamSink(metadata: MetadataInfo, outDir: Path, packages: Seq[(Long, String)]):
  Sink[(DatasetGraph, Long), Seq[Future[SaveResult]]] =
    val sinks = packages.map { case (size, name) =>
      Flow[(String, String)]
        .take(size)
        .toMat(FileHelper.writeArchive(outDir.resolve(s"stream_$name.tar.gz"), size))(Keep.right)
    }
    Sink.fromGraph(GraphDSL.create(sinks) { implicit builder =>
      sinkList =>
      import GraphDSL.Implicits.*
      val writerFlow = Flow[(DatasetGraph, Long)]
        .async
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

  /**
   * Creates a sink that writes the data to flat files
   * @param metadata the metadata of the dataset
   * @param outDir the directory to write the files to
   * @param packages the packages to write (size, name)
   * @return the sink
   */
  private def packageFlatSink(metadata: MetadataInfo, outDir: Path, packages: Seq[(Long, String)]):
  Sink[(DatasetGraph, Long), Seq[Future[SaveResult]]] =
    val fileExtension = if metadata.elementType == "triples" then "nt" else "nq"

    val sinks = packages.map { case (size, name) =>
      Flow[ByteString]
        .take(size)
        .via(Compression.gzip)
        .toMat(FileHelper.fileSink(outDir.resolve(s"flat_$name.$fileExtension.gz")))(Keep.right)
    }

    Sink.fromGraph(GraphDSL.create(sinks) { implicit builder =>
      sinkList =>
      import GraphDSL.Implicits.*
      val serializeFlow = Flow[(DatasetGraph, Long)]
        .async
        .map((ds, _) => {
          if metadata.elementType == "triples" then
            RDFWriter.create()
              .lang(Lang.NTRIPLES)
              .source(ds.getDefaultGraph)
              .asString()
          else
            RDFWriter.create()
              .lang(Lang.NQUADS)
              .source(ds)
              .asString()
        })
        .map(ByteString.fromString)

      val serializeFlowG = builder.add(serializeFlow)
      val serializeBroad = builder.add(Broadcast[ByteString](sinkList.size))
      serializeFlowG.out ~> serializeBroad
      for sink <- sinkList do
        serializeBroad ~> sink

      SinkShape(serializeFlowG.in)
    })

  /**
   * Checks the structure of the dataset
   * @param metadata the metadata of the dataset
   * @return a flow that checks the structure of the dataset
   */
  private def checkStructureFlow(metadata: MetadataInfo): Flow[(DatasetGraph, Long), Unit, NotUsed] =
    Flow[(DatasetGraph, Long)].map((ds, _) => checkStructure(metadata, ds))
      .map({
        case Some(msg) => throw new Exception(msg)
        case None => ()
      })

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
