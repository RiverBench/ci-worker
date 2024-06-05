package io.github.riverbench.ci_worker
package commands.batch

import commands.*
import util.*
import util.io.*

import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import eu.ostrzyciel.jelly.convert.jena.given
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
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
      |Args: <profile-name> <version> <distribution-size> <output-nodes (0/1)> <out-dir>
      |""".stripMargin

  override def validateArgs(args: Array[String]): Boolean = args.length == 6

  override def run(args: Array[String]): Future[Unit] = Future {
    val profileName = args(1)
    val version = args(2)
    val distribution = args(3)
    val outputNodes = args(4).toInt
    val outDir = Path.of(args(5))

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
        Await.ready(processDataset(dataset, distribution, datasetOutDir, outputNodes), 10.hours)
  }


  private def processDataset(datasetIri: Resource, distributionSize: String, outDir: Path, outputNodes: Int): 
  Future[Unit] =
    val datasetM = RDFDataMgr.loadModel(datasetIri.getURI, Lang.RDFXML)
    val distribution = datasetM.listSubjectsWithProperty(RdfUtil.dctermsIdentifier, f"jelly-$distributionSize")
      .asScala.toSeq.head
    val downloadUrl = distribution.getPropertyResourceValue(RdfUtil.dcatDownloadURL).getURI

    println(f"Processing data from $downloadUrl...")
    val response = HttpHelper.getWithFollowRedirects(downloadUrl)
    
    class LongMutable(var value: Long)

    val sMap = new mutable.HashMap[Node, LongMutable]()
    val pMap = new mutable.HashMap[Node, LongMutable]()
    val oMap = new mutable.HashMap[Node, LongMutable]()
    val gMap = new mutable.HashMap[Node, LongMutable]()

    val maps = Seq(
      "subject" -> sMap,
      "predicate" -> pMap,
      "object" -> oMap,
      "graph" -> gMap
    )

    def writeToMap(map: mutable.HashMap[Node, LongMutable], node: Node): Unit =
      val counter = map.getOrElseUpdate(node, LongMutable(0L))
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
        writeToMap(sMap, s)
        writeToMap(pMap, p)
        writeToMap(oMap, o)
      case q: Quad =>
        val s = q.getSubject
        val p = q.getPredicate
        val o = q.getObject
        val g = q.getGraph
        writeToMap(sMap, s)
        writeToMap(pMap, p)
        writeToMap(oMap, o)
        writeToMap(gMap, g)
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
        for (node, counter) <- map do
          if outputNodes == 1 then
            os.write(f"${node.toString(true)}\t${counter.value}\n")
          else
            os.write(f"${counter.value}\n")
        os.close()

      println("Done.")
    }
