package io.github.riverbench.ci_worker
package commands

import util.{ArchiveHelper, Constants, MetadataReader}

import akka.NotUsed
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.stream.scaladsl.*
import akka.util.ByteString

import java.nio.file.FileSystems
import scala.concurrent.Await

object StreamPackageCommand extends Command:
  override def name = "stream-package"

  override def description = "Package a dataset as a stream.\n" +
    "Args: <repo dir>"

  override def validateArgs(args: Array[String]) = args.length == 2

  override def run(args: Array[String]): Unit =
//    val repoDir = FileSystems.getDefault.getPath(args(1))
//    val dataFile = ArchiveHelper.findDataFile(repoDir)
//    val metadata = MetadataReader.read(repoDir)
//
//    val packages = Constants.packageSizes
//      .filter(_ <= metadata.elementCount)
//      .map(s => (s, Constants.packageSizeToHuman(s)))
//      ++ Seq((metadata.elementCount, "full"))
//
//    val sinks = packages.map { case (size, name) =>
//      Flow[(TarArchiveMetadata, Source[ByteString, NotUsed])]
//        .take(size)
//        .toMat(ArchiveHelper.write(repoDir.resolve(s"stream_$name.tar.gz"), size))(Keep.right)
//    }

//    val ioFuture = ArchiveHelper.read(dataFile)
////      .mapAsync((m, byteStream) => {
////        byteStream.runWith(Sink.seq).onComplete { buffer =>
////          (m, Source.fromIterator(() => buffer.iterator))
////        }
////      })
//      .runWith(ArchiveHelper.write(repoDir.resolve("stream_copy.tar.gz"), metadata.elementCount))

//    Await.ready(ioFuture, scala.concurrent.duration.Duration.Inf)
    println("Done!")
