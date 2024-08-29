package io.github.riverbench.ci_worker
package util.io

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.connectors.file.TarArchiveMetadata
import org.apache.pekko.stream.connectors.file.scaladsl.Archive
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.util.ByteString

import java.nio.file.Path
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object FileHelper:

  def readArchiveFromUrl(url: String)(implicit as: ActorSystem[_]):
  Source[(TarArchiveMetadata, ByteString), NotUsed] =
    val response = HttpHelper.getWithFollowRedirects(url)
    given ExecutionContext = as.executionContext
    readArchive(
      Source.futureSource(response.map(r => r.entity.dataBytes))
    )

  def readArchiveFromFile(path: Path)(implicit as: ActorSystem[_]):
  Source[(TarArchiveMetadata, ByteString), NotUsed] =
    readArchive(FileIO.fromPath(path))

  /**
   * Reads a source data archive from a byte source.
   * @param source the source of the archive data
   * @return a source of the archive entries
   */
  def readArchive(source: Source[ByteString, _])(implicit as: ActorSystem[_]):
  Source[(TarArchiveMetadata, ByteString), NotUsed] =
    given ExecutionContext = as.executionContext
    // Unfortunately, Pekko untar stage is glitchy with large archives, so we have to
    // use the non-reactive Apache Commons implementation instead.
    val tarIs = source
      .via(Compression.gunzip())
      .toMat(StreamConverters.asInputStream())(Keep.right)
      .mapMaterializedValue(bytesIs => TarArchiveInputStream(bytesIs))
      .run()

    val tarIterator = Iterator
      .continually(tarIs.getNextEntry)
      .takeWhile(_ != null)
      .filter(tarIs.canReadEntryData)
      .filter(_.isFile)
      .map(entry => (
        TarArchiveMetadata(
          filePathPrefix = "",
          filePathName = entry.getName,
          size = entry.getSize,
          lastModification = Instant.ofEpochMilli(entry.getModTime.getTime),
        ),
        ByteString.fromArray(IOUtils.toByteArray(tarIs))
      ))

    Source.fromIterator(() => tarIterator)

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
    val dirLevels = Math.floor(Math.log10(len.toDouble - 1) / 3).toInt

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
          // Set modification time to 1970 to make the output ideally stable.
          // See: https://github.com/RiverBench/RiverBench/issues/81
          lastModification = Instant.ofEpochSecond(0),
        )
        extraDirs.map(extraDir => (
          TarArchiveMetadata.directory(
            extraDir,
            Instant.ofEpochSecond(0),
          ),
          Source.empty[ByteString]
        )) ++ Seq(
          (metadata, Source(Seq(ByteString(bytes))))
        )
      })
      .via(Archive.tar())
      .toMat(writeCompressed(file))(Keep.right)

  /**
   * Sink for writing a compressed file. It does not calculate the size or checksums.
   * @param file the path to the file
   * @param ec the execution context
   * @return a sink that writes to the file
   */
  def writeCompressed(file: Path)(implicit ec: ExecutionContext): Sink[ByteString, Future[SaveResult]] =
    Flow[ByteString]
      .groupedWeighted(2 * 1024 * 1024)(_.size)
      .map(_.reduce(_ ++ _))
      .via(Compression.gzip)
      .async
      .toMat(fileSink(file))(Keep.right)
