package io.github.riverbench.ci_worker
package util.doc

import util.Constants

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.jdk.CollectionConverters.*

object DocFileUtil:
  def copyDocs(sourceDir: Path, targetDir: Path, ignoreFiles: Seq[String] = Seq()): Unit =
    Files.createDirectories(targetDir)
    if Files.exists(sourceDir) then
      val docFiles = Files.list(sourceDir).iterator().asScala
        .filter(f => Files.isRegularFile(f) &&
          Constants.allowedDocExtensions.contains(f.getFileName.toString.split('.').last) &&
          !ignoreFiles.contains(f.getFileName.toString)
        )
        // Only files smaller than 2 MB
        .filter(f => Files.size(f) < 2 * 1024 * 1024)
        .toSeq
      for docFile <- docFiles do
        val target = targetDir.resolve(docFile.getFileName)
        Files.copy(docFile, target, StandardCopyOption.REPLACE_EXISTING)
        println(s"Copied $docFile to $target")
