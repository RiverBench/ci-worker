package io.github.riverbench.ci_worker
package util.collection

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr

object CategoryCollection:
  def fromReleases(names: Iterable[String], version: String): CategoryCollection =
    val categories = names.map { name =>
      if version == "latest" then
        (name, s"https://github.com/RiverBench/category-$name/releases/latest/download/metadata.ttl")
      else
        (name, s"https://github.com/RiverBench/category-$name/releases/download/$version/metadata.ttl")
    }
    CategoryCollection(categories)

class CategoryCollection(namesToUris: Iterable[(String, String)]):
  val categories: Map[String, Model] = namesToUris
    .map((name, uri) => {
      try {
        (name, RDFDataMgr.loadModel(uri))
      }
      catch {
        case e: Exception =>
          println(f"Failed to load metadata for category $name from $uri")
          throw e
      }
    })
    .toMap
