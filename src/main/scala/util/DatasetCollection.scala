package io.github.riverbench.ci_worker
package util

import org.apache.jena.riot.RDFDataMgr

import java.nio.file.Path

object DatasetCollection:
  def fromDirectory(datasetDir: Path): DatasetCollection =
    val datasets = datasetDir.toFile.listFiles
      .filter(_.isDirectory)
      .map(dDir => {
        val metadataPath = dDir.toPath.resolve("metadata.ttl")
        (dDir.getName, metadataPath.toString)
      })
    DatasetCollection(datasets)

  def fromReleases(names: Iterable[String], version: String): DatasetCollection =
    val datasets = names.map { name =>
      if version == "latest" then
        (name, s"https://github.com/RiverBench/dataset-$name/releases/latest/download/metadata.ttl")
      else
        (name, s"https://github.com/RiverBench/dataset-$name/releases/download/$version/metadata.ttl")
    }
    DatasetCollection(datasets)

class DatasetCollection(namesToUris: Iterable[(String, String)]):
  val datasets = namesToUris
    .map((name, uri) => {
      try {
        (name, RDFDataMgr.loadModel(uri))
      }
      catch {
        case e: Exception =>
          println(f"Failed to load metadata for dataset $name from $uri")
          throw e
      }
    })
    .toMap
