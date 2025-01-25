package io.github.riverbench.ci_worker
package commands.batch

import commands.*
import util.*
import util.io.*
import util.rdf.*

import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import eu.ostrzyciel.jelly.convert.jena.given
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import org.apache.datasketches.cpc.*
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Quad
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Location
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.*

import java.io.{BufferedWriter, FileOutputStream, FileWriter}
import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object DegreeDistributionsCommand extends Command:
  override def name: String = "batch-degree-distribution"

  override def description: String =
    """Batch command, not intended for use in the CI.
      |Computes the node degree distributions for datasets from a given RiverBench profile.
      |
      |This command uses A LOT of system memory (~30–40 GB with full-size distributions in RB 2.0.0).
      |As output it provides cardinality estimates for the number of unique triples in which subject, predicate,
      |object, and graph node appears. The estimates are provided with lower and upper bounds with 95% confidence.
      |
      |Args: <profile-name> <version> <distribution-size> <output-nodes (0/1)> <detailed (0/1)> <out-dir>
      |""".stripMargin

  override def validateArgs(args: Array[String]): Boolean = args.length == 7

  override def run(args: Array[String]): Future[Unit] = Future {
    val profileName = args(1)
    val version = args(2)
    val distribution = args(3)
    val outputNodes = args(4).toInt
    val detailed = args(5).toInt
    val outDir = Path.of(args(6))

    println(f"Fetching profile $profileName version $version...")
    val profileIri = PurlMaker.profile(profileName, version)
    val profileM = RDFDataMgr.loadModel(profileIri, Lang.RDFXML)

    val datasets = profileM.listObjectsOfProperty(RdfUtil.dcatSeriesMember).asScala.map(_.asResource).toSeq
    println(s"Will process datasets:${datasets.map(_.toString).fold("")((a, b) => a + " " + b)}")

    val futures = for dataset <- datasets yield
      val iriParts = dataset.getURI.split('/')
      val datasetOutDir = outDir.resolve(iriParts(iriParts.length - 2))
      if datasetOutDir.toFile.exists() && Files.list(datasetOutDir).count() > 0 then
        println(f"Skipping dataset ${dataset.getURI}, output directory already exists and is not empty.")
        Future.successful(())
      else
        datasetOutDir.toFile.mkdirs()
        Await.ready(processDataset(dataset, distribution, datasetOutDir, outputNodes, detailed), 10.hours)
  }


  private def processDataset(datasetIri: Resource, distributionSize: String, outDir: Path, outputNodes: Int, detailed: Int): 
  Future[Unit] =
    val datasetM = RDFDataMgr.loadModel(datasetIri.getURI, Lang.RDFXML)
    val distribution = datasetM.listSubjectsWithProperty(RdfUtil.dctermsIdentifier, f"jelly-$distributionSize")
      .asScala.toSeq.head
    val downloadUrl = distribution.getPropertyResourceValue(RdfUtil.dcatDownloadURL).getURI

    println(f"Processing data from $downloadUrl...")
    val response = HttpHelper.getWithFollowRedirects(downloadUrl)
    
    class LongMutable(var value: Long)

    val sMap = new mutable.HashMap[Node, (CpcSketch, LongMutable)]()
    val pMap = new mutable.HashMap[Node, (CpcSketch, LongMutable)]()
    val oMap = new mutable.HashMap[Node, (CpcSketch, LongMutable)]()
    val gMap = new mutable.HashMap[Node, (CpcSketch, LongMutable)]()
    val stMap = new mutable.HashMap[Node, (CpcSketch, LongMutable)]()

    val maps = if detailed == 1 then Seq(
      "subject" -> sMap,
      "predicate" -> pMap,
      "object" -> oMap,
      "graph" -> gMap
    ) else Seq("statement" -> stMap)

    def writeToMap[T](map: mutable.HashMap[T, (CpcSketch, LongMutable)], key: T, hashes: Array[Int]): Unit =
      val (sketch, counter) = map.getOrElseUpdate(key, (CpcSketch(11), LongMutable(0L)))
      sketch.update(hashes)
      counter.value += 1
    
    val statementSource: Source[Triple | Quad, NotUsed] = Source.futureSource(response.map(_.entity.dataBytes))
      .via(Compression.gunzip())
      .toMat(StreamConverters.asInputStream())(Keep.right)
      .mapMaterializedValue(JellyIo.fromIoStream)
      .run()
      .via(DecoderFlow.decodeAny.asFlatStream)

    val streamFuture = statementSource.runForeach {
      case t: Triple =>
        val s = t.getSubject
        val p = t.getPredicate
        val o = t.getObject
        val hashes = Array(s.hashCode, p.hashCode, o.hashCode)
        if detailed == 1 then
          writeToMap(sMap, s, hashes)
          writeToMap(pMap, p, hashes)
          writeToMap(oMap, o, hashes)
        else
          for n <- Seq(s, p, o) do
            writeToMap(stMap, n, hashes)
      case q: Quad =>
        val s = q.getSubject
        val p = q.getPredicate
        val o = q.getObject
        val g = q.getGraph
        val hashes = Array(s.hashCode, p.hashCode, o.hashCode, g.hashCode)
        if detailed == 1 then
          writeToMap(sMap, s, hashes)
          writeToMap(pMap, p, hashes)
          writeToMap(oMap, o, hashes)
          writeToMap(gMap, g, hashes)
        else
          for n <- Seq(s, p, o, g) do
            writeToMap(stMap, n, hashes)
    }

    streamFuture map { _ =>
      println("Building hashmaps done.")
      Files.writeString(outDir.resolve("stats.yml"),
        f"""subjects: ${sMap.size}
           |predicates: ${pMap.size}
           |objects: ${oMap.size}
           |graphs: ${gMap.size}
           |""".stripMargin)

      for (name, map) <- maps do
        val os = BufferedWriter(FileWriter(outDir.resolve(f"$name.tsv").toFile))
        for (node, (sketch, counter)) <- map do
          val data = f"${counter.value}\t" +
            f"${sketch.getLowerBound(2)}\t${sketch.getEstimate}\t${sketch.getUpperBound(2)}\n"
          if outputNodes == 1 then
            os.write(f"${node.toString}\t$data")
          else
            os.write(data)
        os.close()

      println("Done.")
    }
