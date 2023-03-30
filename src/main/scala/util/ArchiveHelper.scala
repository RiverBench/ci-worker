package io.github.riverbench.ci_worker
package util

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl.*
import akka.util.ByteString

import java.io.BufferedInputStream
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object ArchiveHelper:
  def findDataFile(datasetDir: Path): Path =
    Seq("triples", "quads", "graphs")
      .map(name => datasetDir.resolve(s"data/$name.tar.gz"))
      .filter(Files.exists(_))
      .head

  def read(file: Path): Source[(TarArchiveMetadata, Source[ByteString, NotUsed]), Future[IOResult]] =
    val source = FileIO.fromPath(file)
    source.via(Compression.gunzip())
      .via(Archive.tarReader())
      .filterNot((metadata, _) => metadata.isDirectory)
      .map((metadata, stream) => (metadata, stream))

  def write(file: Path, len: Long):
  Sink[(TarArchiveMetadata, Source[ByteString, NotUsed]), Future[IOResult]] =
    val dirLevels = Math.floor(Math.log10(len.toDouble) / 3).toInt

    def makePathForFile(m: TarArchiveMetadata): (String, Iterable[String]) =
      val name = m.filePath.split('/').last
      val num = name.split('.').head
      var dir = ""
      val extraDirs = ListBuffer[String]()
      for d <- -dirLevels until 0 do
        val currentLevel = num.slice(10 + 3 * (d - 1), 10 + 3 * d) + "/"
        if num.slice(10 + 3 * d, 10).toInt == 0 then
          extraDirs += currentLevel
        dir += currentLevel
      (dir + name, extraDirs)

    Flow[(TarArchiveMetadata, Source[ByteString, NotUsed])]
      .filter((m, _) => !m.isDirectory)
      .mapConcat((m, stream) => {
        val (name, extraDirs) = makePathForFile(m)
        val metadata = TarArchiveMetadata(
          filePathPrefix = "",
          filePathName = name,
          size = m.size,
          lastModification = Instant.now,
          linkIndicatorByte = m.linkIndicatorByte,
        )
        extraDirs.map(extraDir => (
          TarArchiveMetadata.directory(extraDir),
          Source.empty[ByteString]
        )) ++ Seq((metadata, stream))
      })
      .via(Archive.tar())
      .via(Compression.gzip)
      .toMat(FileIO.toPath(file))(Keep.right)
