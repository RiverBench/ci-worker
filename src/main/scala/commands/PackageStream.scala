package io.github.riverbench.ci_worker
package commands

import akka.stream.IOResult
import akka.{Done, NotUsed}
import akka.stream.scaladsl.*
import io.github.riverbench.ci_worker.util.ArchiveReader
import org.apache.jena.query.DatasetFactory
import org.apache.jena.riot.{Lang, RDFDataMgr}

import java.io.InputStream
import java.nio.file.FileSystems
import scala.concurrent.{Await, Future}

object PackageStream extends Command:
  override def name: String = "package-stream"

  override def description = "..."

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val dataFile = ArchiveReader.findDataFile(repoDir)
    val readFuture = ArchiveReader.read(dataFile)
      .map((name, byteStream) => {
        val is = byteStream.runWith(StreamConverters.asInputStream())
        val lang = if name.endsWith(".ttl") then Lang.TTL else Lang.TRIG
        val ds = DatasetFactory.create()
        RDFDataMgr.read(ds, is, lang)
        (name, ds)
      })
      .zipWithIndex






    // Await.ready(readFuture, scala.concurrent.duration.Duration.Inf)
//    val files = Await.result(readFuture, scala.concurrent.duration.Duration.Inf)
//    print(files.size)
//    print(files.size)
//    files.foreach(println)
