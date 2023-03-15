package io.github.riverbench.ci_worker
package commands

import io.github.riverbench.ci_worker.util.ArchiveReader

import java.nio.file.FileSystems
import scala.concurrent.Await

object PackageStream extends Command:
  override def name: String = "package-stream"

  override def description = "..."

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Unit =
    val repoDir = FileSystems.getDefault.getPath(args(1))
    val readFuture = ArchiveReader.read(repoDir.resolve("data/triples.tar.gz"))
    // Await.ready(readFuture, scala.concurrent.duration.Duration.Inf)
//    val files = Await.result(readFuture, scala.concurrent.duration.Duration.Inf)
//    print(files.size)
//    print(files.size)
//    files.foreach(println)
