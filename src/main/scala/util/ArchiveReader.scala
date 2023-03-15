package io.github.riverbench.ci_worker
package util

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl.*
import akka.util.ByteString

import java.io.BufferedInputStream
import java.nio.file.{Files, Path}
import scala.concurrent.Future

object ArchiveReader:
  def findDataFile(datasetDir: Path): Path =
    Seq("triples", "quads", "graphs")
      .map(name => datasetDir.resolve(s"data/$name.tar.gz"))
      .filter(Files.exists(_))
      .head

  def read(file: Path)(implicit as: ActorSystem[_]): Source[(String, Source[ByteString, NotUsed]), Future[IOResult]] =
    val source = StreamConverters.fromInputStream(() => new BufferedInputStream(Files.newInputStream(file)))
    source.via(Compression.gunzip())
      .via(Archive.tarReader())
      .filterNot((metadata, _) => metadata.isDirectory)
      .map((metadata, stream) => (metadata.filePath, stream))
