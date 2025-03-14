package io.github.riverbench.ci_worker
package commands

import util.*
import util.io.*
import util.rdf.*

import eu.ostrzyciel.jelly.core.proto.v1.PhysicalStreamType
import eu.ostrzyciel.jelly.core.{JellyOptions, LogicalStreamTypeFactory}
import eu.ostrzyciel.jelly.stream.{EncoderFlow, JellyIo}
import org.apache.jena.rdf.model.{ModelFactory, Resource}
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.system.{ErrorHandlerFactory, FactoryRDFStd, StreamRDFWriter}
import org.apache.jena.riot.{Lang, RDFParser, RDFWriter}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory}
import org.apache.pekko.stream.*
import org.apache.pekko.stream.connectors.file.TarArchiveMetadata
import org.apache.pekko.stream.scaladsl.{Flow, *}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}
import org.eclipse.rdf4j.model.vocabulary.XSD
import org.eclipse.rdf4j.rio

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream}
import java.nio.file.{FileSystems, Path}
import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PackageCommand extends Command:
  import RdfOrdering.given
  import eu.ostrzyciel.jelly.convert.jena.{*, given}

  sealed trait DistType(val weight: Int)
  object DistType:
    case object Flat extends DistType(0)
    case object Stream extends DistType(1)
    case object Jelly extends DistType(2)

  private case class PartialResult(size: Long, stats: StatCounterSuite.Result, saveRes: SaveResult, dType: DistType)

  override def name: String = "package"

  override def description = "Packages a dataset.\n" +
    "Args: <repo-dir> <source archive file> <output-dir>"

  override def validateArgs(args: Array[String]) = args.length == 4

  override def run(args: Array[String]): Future[Unit] = Future {
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val sourceArchiveFile = FileSystems.getDefault.getPath(args(2))
    val outDir = FileSystems.getDefault.getPath(args(3))
    
    val metadata = MetadataReader.read(repoDir)
    val stats = new StatCounterSuite(metadata.elementCount)
    val packages = Constants.packageSizes
      .filter(_ <= metadata.elementCount)
      .map(s => (s, Constants.packageSizeToHuman(s)))
      ++ Seq((metadata.elementCount, "full"))

    val sinkStats = Sink.seq[(Long, StatCounterSuite.Result)]
    val sinkStreamPackage = packageStreamSink(metadata, outDir, packages)
    val sinkFlatPackage = packageFlatSink(metadata, outDir, packages)
    val sinkJellyPackage = packageJellySink(metadata, outDir, packages)
    val sinkSampleStream = streamSamplesSink(metadata, outDir)
    val sinkChecks = Sink.ignore

    val g = RunnableGraph.fromGraph(
      GraphDSL.createGraph(
        sinkStats, sinkStreamPackage, sinkFlatPackage, sinkJellyPackage, sinkSampleStream, sinkChecks
      )((_, _, _, _, _, _))
    { implicit builder =>
      (sStats, sStreamPackage, sFlatPackage, sJellyPackage, sSampleStream, sChecks) =>
      import GraphDSL.Implicits.*
      val in = FileHelper.readArchiveFromFile(sourceArchiveFile)
        .map((name, bytes) => (name, bytes.utf8String))
        .async
        .buffer(16, OverflowStrategy.backpressure)
      val inBroad = builder.add(Broadcast[(TarArchiveMetadata, String)](2))
      val parseJenaBuffered = parseJenaFlow
        .zipWithIndex
        .buffer(32, OverflowStrategy.backpressure)
      val dsBroad = builder.add(Broadcast[(DatasetGraph, Long)](6))
      val flatSerialize = packageFlatSerializeFlow(metadata)
      val flatSerializeBroad = builder.add(Broadcast[ByteString](2))
      val statsZip = builder.add(Zip[ByteString, (DatasetGraph, Long)]())
      val checksMerge = builder.add(Merge[Unit](2))

      in ~> inBroad
      inBroad ~> checkRdf4jFlow(metadata).async ~> checksMerge ~> sChecks
      inBroad ~> parseJenaBuffered.async ~> dsBroad ~> checkStructureFlow(metadata).async ~> checksMerge
      dsBroad ~> flatSerialize ~> flatSerializeBroad ~> sFlatPackage
      flatSerializeBroad ~> statsZip.in0
      dsBroad ~> statsZip.in1
      statsZip.out ~> statsFlow(stats).async ~> sStats
      dsBroad ~> sStreamPackage
      dsBroad ~> sJellyPackage
      dsBroad ~> sSampleStream

      ClosedShape
    }).mapMaterializedValue((fStats, fSeqStreamRes, fSeqFlatRes, fSeqJellyRes, fSeqSampleStream, fChecks) => {
      val fStreamRes = Future.sequence(fSeqStreamRes)
      val fFlatRes = Future.sequence(fSeqFlatRes)
      val fJellyRes = Future.sequence(fSeqJellyRes)
      val fSampleStreamRes = Future.sequence(fSeqSampleStream)
      for
        // There is no value returned from the checks sink, but we need to ensure it is run
        _ <- fChecks
        stats <- fStats
        streamRes <- fStreamRes
        flatRes <- fFlatRes
        jellyRes <- fJellyRes
        // Ignore the results from sample streams
        _ <- fSampleStreamRes
      yield
        val sr = for ((num, stat), streamR) <- stats.zip(streamRes) yield
          PartialResult(num, stat, streamR, DistType.Stream)
        val fr = for ((num, stat), flatR) <- stats.zip(flatRes) yield
          PartialResult(num, stat, flatR, DistType.Flat)
        val jr = for ((num, stat), jellyR) <- stats.zip(jellyRes) yield
          PartialResult(num, stat, jellyR, DistType.Jelly)
        sr ++ fr ++ jr
    })

    val m = ModelFactory.createDefaultModel()
    m.setNsPrefix("dcat", RdfUtil.pDcat)
    m.setNsPrefix("dcterms", RdfUtil.pDcterms)
    m.setNsPrefix("rb", RdfUtil.pRb)
    m.setNsPrefix("rbdoc", RdfUtil.pRbDoc)
    m.setNsPrefix("rdfs", RdfUtil.pRdfs)
    m.setNsPrefix("spdx", RdfUtil.pSpdx)
    m.setNsPrefix("stax", RdfUtil.pStax)
    m.setNsPrefix("xsd", XSD.NAMESPACE)

    val datasetRes = m.createResource(RdfUtil.tempDataset.getURI)

    (g.run(), m, datasetRes, outDir, metadata)
  } flatMap { (pResultsF, m, datasetRes, outDir, metadata) =>
    pResultsF map { pResults =>
      val statResources = pResults.filter(_.dType == DistType.Flat)
        .map(pr => pr.size -> pr.stats.addToRdf(m, pr.size, metadata.elementCount)).toMap

      metadata.addStreamTypesToRdf(datasetRes)
      for pResult <- pResults do
        distributionToRdf(datasetRes, statResources(pResult.size), metadata, pResult)

      val statsFile = outDir.resolve("package_metadata.ttl").toFile
      val os = new FileOutputStream(statsFile)

      m.write(os, "TURTLE")
      println("Done.")
    }
  }

  /**
   * Adds RDF metadata for a given distribution
   * @param datasetRes the dataset resource
   * @param statRes the statistics resource
   * @param mi dataset metadata
   * @param pResult partial result for the distribution
   */
  private def distributionToRdf(datasetRes: Resource, statRes: Resource, mi: MetadataInfo, pResult: PartialResult):
  Unit =
    val m = datasetRes.getModel
    val distRes = m.createResource(RdfUtil.tempDataset.getURI + "#" + pResult.saveRes.getLocalId)
    datasetRes.addProperty(RdfUtil.dcatDistribution, distRes)
    mi.addToRdf(distRes, pResult.size, pResult.dType)
    distRes.addProperty(RdfUtil.hasFileName, pResult.saveRes.name)
    distRes.addProperty(RdfUtil.hasStatisticsSet, statRes)
    distRes.addProperty(RdfUtil.dcatIsDistributionOf, datasetRes)
    pResult.saveRes.addToRdf(distRes, mi, pResult.dType)

  /**
   * Creates a flow for parsing the input stream with Jena to DatasetGraphs
   * @return a flow that parses the input stream
   */
  private def parseJenaFlow: Flow[(TarArchiveMetadata, String), DatasetGraph, NotUsed] =
    // Override the mechanism for creating blank nodes to make their IDs deterministic
    // As long as the seed stays the same, the blank node IDs parsed from the same file will be the same.
    // See: https://github.com/RiverBench/RiverBench/issues/81
    val labelToNode = LabelToNode.createScopeByDocumentHash(UUID.fromString("13371337-1337-1337-1337-000000000000"))
    val rdfFactory = FactoryRDFStd(labelToNode)

    Flow[(TarArchiveMetadata, String)].map((tarMeta, data) => {
      val name = tarMeta.filePathName
      val lang = if name.endsWith(".ttl") then Lang.TTL else Lang.TRIG
      val ds = DatasetGraphFactory.create()

      // Parse the dataset in Jena
      try {
        RDFParser.create()
          // most strict parsing settings possible
          .checking(true)
          .factory(rdfFactory)
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
  Flow[(ByteString, (DatasetGraph, Long)), (Long, StatCounterSuite.Result), NotUsed] =
    Flow[(ByteString, (DatasetGraph, Long))]
      .wireTap((_, y) => if (y._2 + 1) % 100_000 == 0 then println(s"Stats stream at: ${y._2 + 1}"))
      .splitAfter((_, y) =>
        val shouldSplit = Constants.packageSizes.contains(y._2 + 1)
        if shouldSplit then println(s"Splitting stats stream at ${y._2 + 1}")
        shouldSplit
      )
      .withAttributes(ActorAttributes.supervisionStrategy(Supervision.stoppingDecider))
      .map((bytes, y) => {
        val (ds, num) = y
        stats.add(ds, bytes)
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
    val format = mi.streamTypes.find(_.isFlat).get.elementType match
      case ElementType.Triple =>
        if mi.conformance.usesRdfStar then rio.RDFFormat.TURTLESTAR else rio.RDFFormat.TURTLE
      case _ =>
        if mi.conformance.usesRdfStar then rio.RDFFormat.TRIGSTAR else rio.RDFFormat.TRIG

    Flow[(TarArchiveMetadata, String)]
      .map((_, data) => {
        val rioErrListener = new rio.helpers.ParseErrorCollector()
        val parser = rio.Rio.createParser(format)
          .setParseErrorListener(rioErrListener)
          .setRDFHandler(Rdf4jUtil.BlackHoleRdfHandler)

        try {
          parser.parse(new ByteArrayInputStream(data.getBytes()))
        } catch {
          case e =>
            println(s"Error parsing $name")
            e.printStackTrace()
            throw e
        }

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
    Flow[(DatasetGraph, Long)]
      .map((ds, num) => {
        if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then
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
      .toMat(StreamUtil.broadcastSink(sinks))(Keep.right)

  /**
   * Creates a sink that writes sample elements of the stream.
   * @param metadata the metadata of the dataset
   * @param outDir the directory to write the files to
   * @return the sink
   */
  private def streamSamplesSink(metadata: MetadataInfo, outDir: Path):
  Sink[(DatasetGraph, Long), Seq[Future[IOResult]]] =
    val extension = if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then "ttl" else "trig"
    val sinks = Constants.streamSamples.map { num =>
      val fileSink = FileIO.toPath(outDir.resolve(f"sample_$num%010d.$extension"))
      Flow[(DatasetGraph, Long)]
        .drop(num)
        .take(1)
        .map((ds, _) => {
          if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then
            RDFWriter.create()
              .lang(Lang.TTL)
              .source(ds.getDefaultGraph)
              .asString()
          else
            RDFWriter.create()
              .lang(Lang.TRIG)
              .source(ds)
              .asString()
        })
        .map(ByteString.fromString)
        .toMat(fileSink)(Keep.right)
    }
    StreamUtil.broadcastSink(sinks)

  private def packageFlatSerializeFlow(metadata: MetadataInfo): Flow[(DatasetGraph, Long), ByteString, NotUsed] =
    Flow[(DatasetGraph, Long)]
      .map((ds, _) => ds.find().asScala.toSeq.sorted)
      .map(quads => {
        val os = ByteArrayOutputStream()
        if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then
          val writer = StreamRDFWriter.getWriterStream(os, Lang.NTRIPLES)
          quads.foreach(q => writer.triple(q.asTriple))
          writer.finish()
        else
          val writer = StreamRDFWriter.getWriterStream(os, Lang.NQUADS)
          quads.foreach(q => writer.quad(q))
          writer.finish()
        os.toByteArray
      })
      .map(ByteString.fromArrayUnsafe)

  /**
   * Creates a sink that writes the data to flat files
   * @param metadata the metadata of the dataset
   * @param outDir the directory to write the files to
   * @param packages the packages to write (size, name)
   * @return the sink
   */
  private def packageFlatSink(metadata: MetadataInfo, outDir: Path, packages: Seq[(Long, String)]):
  Sink[ByteString, Seq[Future[SaveResult]]] =
    val fileExtension = if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then "nt" else "nq"
    val sinks = packages.map { case (size, name) =>
      Flow[ByteString]
        .take(size)
        .toMat(FileHelper.writeCompressed(outDir.resolve(s"flat_$name.$fileExtension.gz")))(Keep.right)
    }
    Flow[ByteString]
      .toMat(StreamUtil.broadcastSink(sinks))(Keep.right)

  /**
   * Creates a sink that writes the data to Jelly-format files.
   * @param metadata the metadata of the dataset
   * @param outDir the directory to write the files to
   * @param packages the packages to write (size, name)
   * @return the sink
   */
  private def packageJellySink(metadata: MetadataInfo, outDir: Path, packages: Seq[(Long, String)]):
  Sink[(DatasetGraph, Long), Seq[Future[SaveResult]]] =
    val sinks = packages.map { case (size, name) =>
      Flow[ByteString]
        .take(size)
        .toMat(FileHelper.writeCompressed(outDir.resolve(s"jelly_$name.jelly.gz")))(Keep.right)
    }

    // Large target message size to avoid splitting
    val jOpt = JellyOptions.bigStrict
      .withGeneralizedStatements(
        metadata.conformance.usesGeneralizedTriples || metadata.conformance.usesGeneralizedRdfDatasets
      )
      .withRdfStar(metadata.conformance.usesRdfStar)
      .withPhysicalType(
        if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then PhysicalStreamType.TRIPLES
        else PhysicalStreamType.QUADS
      )
      // Add RDF-STaX stream type (logical stream type in RDF-STaX)
      .withLogicalType(LogicalStreamTypeFactory.fromOntologyIri(
        metadata.streamTypes.find(!_.isFlat).get.iri
      ).get)

    val serializeFlow = (
      if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then
        Flow[(DatasetGraph, Long)]
          .map((ds, _) => ds.getDefaultGraph.asTriples.toSeq.sorted)
          .via(EncoderFlow.builder.graphs(jOpt).flow)
      else
        Flow[(DatasetGraph, Long)]
          .map((ds, _) => ds.asQuads.toSeq.sorted)
          .via(EncoderFlow.builder.datasetsFromQuads(jOpt).flow)
      )
      .via(JellyIo.toBytesDelimited)
      .map(ByteString(_))

    serializeFlow.toMat(StreamUtil.broadcastSink(sinks))(Keep.right)

  /**
   * Checks the structure of the dataset
   * @param metadata the metadata of the dataset
   * @return a flow that checks the structure of the dataset
   */
  private def checkStructureFlow(metadata: MetadataInfo): Flow[(DatasetGraph, Long), Unit, NotUsed] =
    Flow[(DatasetGraph, Long)].map((ds, num) => (checkStructure(metadata, ds), num))
      .map({
        case (Some(msg), num) => throw new Exception(f"Element $num:\n" + msg)
        case (None, _) => ()
      })

  private def checkStructure(metadata: MetadataInfo, ds: DatasetGraph): Option[String] =
    if metadata.streamTypes.exists(_.elementType == ElementType.Triple) then
      // Only the default graph is allowed
      if ds.listGraphNodes().asScala.toSeq.nonEmpty then
        return Some("There are named graphs in a triple stream dataset")
    else if metadata.streamTypes.contains(StreamType.NamedGraph) ||
      metadata.streamTypes.contains(StreamType.TimestampedNamedGraph) then
      // One named graph is allowed + the default graph
      val graphs = ds.listGraphNodes().asScala.toSeq
      if graphs.size != 1 then
        return Some("There must be exactly one named graph in a graphs dataset")

      metadata.temporalProp match
        case Some(tProp) =>
          ds.getDefaultGraph.find(graphs.head, tProp.asNode, null).asScala.toSeq match
            case Seq() => return Some(s"The temporal property $tProp is not present in the dataset")
            case Seq(_) => ()
            case _ => return Some(s"The temporal property $tProp is present multiple times in the dataset")
        case None => ()

    if metadata.subjectNodeShapes.nonEmpty then
      metadata.subjectNodeShapes.flatMap(_.findInDs(ds)).toSet match
        case s if s.isEmpty => return Some(
          s"The subject node (selected by shapes ${metadata.subjectNodeShapes}) is not present in the dataset"
        )
        case s if s.size == 1 => ()
        case _ => return Some(
          s"The subject node (selected by shapes ${metadata.subjectNodeShapes}) is present multiple times in the dataset"
        )

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
