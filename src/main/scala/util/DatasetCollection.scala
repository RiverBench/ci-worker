package io.github.riverbench.ci_worker
package util

import org.apache.jena.riot.RDFDataMgr

import java.nio.file.Path

class DatasetCollection(datasetDir: Path):
  val datasets = datasetDir.toFile.listFiles
    .filter(_.isDirectory)
    .map(dDir => {
      val metadataPath = dDir.toPath.resolve("metadata.ttl")
      (dDir.getName, RDFDataMgr.loadModel(metadataPath.toString))
    })
    .toMap
