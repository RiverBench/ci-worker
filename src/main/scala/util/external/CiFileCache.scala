package io.github.riverbench.ci_worker
package util.external

import util.AppConfig

import java.io.File
import java.nio.file.Path

object CiFileCache:
  private val baseDir = Path.of(AppConfig.CiWorker.cacheDir)

  def getCachedFilesInDir(dirName: String): Seq[File] =
    val dir = baseDir.resolve(dirName).toFile
    if !dir.exists() then
      dir.mkdirs()
    dir.listFiles().toSeq

  def deleteOldCachedFiles(dirName: String, keep: Iterable[String]): Unit =
    val existingFiles = getCachedFilesInDir(dirName)
    val filesToDelete = existingFiles.filterNot(f => keep.exists(f.getName.endsWith))
    filesToDelete.foreach(f => {
      f.delete()
      println(s"Deleted old cached file: $dirName/${f.getName}")
    })

  def getCachedFile(dirName: String, fileName: String): File =
    val dir = baseDir.resolve(dirName)
    if !dir.toFile.exists() then
      dir.toFile.mkdirs()
    dir.resolve(fileName).toFile
