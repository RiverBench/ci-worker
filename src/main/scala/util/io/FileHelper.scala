package io.github.riverbench.ci_worker
package util.io

import akka.actor.typed.ActorSystem
import akka.stream.*
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl.*
import akka.util.ByteString
import akka.{Done, NotUsed}

import java.io.BufferedInputStream
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object FileHelper:

  /**
   * Finds the source data file in the dataset directory.
   * @param datasetDir the dataset directory
   * @return the path to the data file
   */
  def findDataFile(datasetDir: Path): Path =
    Seq("triples", "quads", "graphs")
      .map(name => datasetDir.resolve(s"data/$name.tar.gz"))
      .filter(Files.exists(_))
      .head

  /**
   * Reads a source data archive.
   * @param file the path to the archive
   * @return a source of the archive entries
   */
  def readArchive(file: Path): Source[(TarArchiveMetadata, Source[ByteString, NotUsed]), Future[IOResult]] =
    val source = FileIO.fromPath(file)
    source.via(Compression.gunzip())
      .via(Archive.tarReader())
      .filterNot((metadata, _) => metadata.isDirectory)
      .map((metadata, stream) => (metadata, stream))

  /**
   * Reusable sink for files that calculates the size, md5 and sha1 checksums.
   * @param file the path to the file
   * @param ec the execution context
   * @return a sink that writes to the file and calculates the size and checksums
   */
  def fileSink(file: Path)(implicit ec: ExecutionContext): Sink[ByteString, Future[SaveResult]] =
    val sinkIO = FileIO.toPath(file)
    val sinkSize = Flow[ByteString]
      .map(_.size)
      .fold(0L)(_ + _)
      .toMat(Sink.head)(Keep.right)
    val sinkMd5 = DigestCalculator.hexString(DigAlgorithm.MD5)
      .toMat(Sink.head)(Keep.right)
    val sinkSha1 = DigestCalculator.hexString(DigAlgorithm.`SHA-1`)
      .toMat(Sink.head)(Keep.right)

    Sink.fromGraph(GraphDSL.createGraph(sinkIO, sinkSize, sinkMd5, sinkSha1)
    ((_, _, _, _)) { implicit b =>
      (sIO, sSize, sMd5, sSha1) =>
        import GraphDSL.Implicits.*
        val bCast = b.add(Broadcast[ByteString](4))
        bCast ~> sIO
        bCast ~> sSize
        bCast ~> sMd5
        bCast ~> sSha1
        SinkShape(bCast.in)
    }).mapMaterializedValue((fIo, fSize, fMd5, fSha1) => {
      for
        io <- fIo
        size <- fSize
        md5 <- fMd5
        sha1 <- fSha1
      yield SaveResult(io, file.getFileName.toString, size, md5, sha1)
    })

  /**
   * Sink for writing a distribution data archive.
   * @param file the path to the archive
   * @param len the number of entries in the archive
   * @param ec the execution context
   * @return a sink that writes to the archive of (filename, data) pairs
   */
  def writeArchive(file: Path, len: Long)(implicit ec: ExecutionContext):
  Sink[(String, String), Future[SaveResult]] =
    val dirLevels = Math.floor(Math.log10(len.toDouble) / 3).toInt

    def makePathForFile(name: String): (String, Iterable[String]) =
      val num = name.split('.').head
      var dir = ""
      val extraDirs = ListBuffer[String]()
      for d <- -dirLevels until 0 do
        val currentLevel = num.slice(10 + 3 * (d - 1), 10 + 3 * d) + "/"
        if num.slice(10 + 3 * d, 10).toInt == 0 then
          extraDirs += currentLevel
        dir += currentLevel
      (dir + name, extraDirs)

    Flow[(String, String)]
      .mapConcat((name, data) => {
        val bytes = data.getBytes
        val (path, extraDirs) = makePathForFile(name)
        val metadata = TarArchiveMetadata(
          filePathPrefix = "",
          filePathName = path,
          size = bytes.length,
          lastModification = Instant.now,
        )
        extraDirs.map(extraDir => (
          TarArchiveMetadata.directory(extraDir),
          Source.empty[ByteString]
        )) ++ Seq(
          (metadata, Source(Seq(ByteString(bytes))))
        )
      })
      .via(Archive.tar())
      .via(Compression.gzip)
      .toMat(fileSink(file))(Keep.right)
