package io.github.riverbench.ci_worker
package util.external

import org.apache.jena.query.QuerySolution
import org.apache.jena.rdfconnection.RDFConnection

import java.nio.file.{Files, Path}
import scala.util.Using

object NanopublicationSparql:
  private val endpoints = Seq(
    "https://virtuoso.nps.knowledgepixels.com/sparql",
    "http://130.60.24.146:7883/sparql",
    "https://virtuoso.services.np.trustyuri.net/sparql",
  )
  private var preferredEndpoint = (0, endpoints.head)
  private val baseQueryString = 
    String(getClass.getResourceAsStream("/sparql/getNanopubsForCategory.rq").readAllBytes())
  

  def getForCategory(category: String): Seq[String] =
    val query = parametrizeQuery(baseQueryString, Map("CATEGORY" -> f"'$category'"))
    var done = false
    val nanopubs = collection.mutable.ArrayBuffer.empty[String]
    while !done do
      try {
        execQuery(query, preferredEndpoint._2, qs => nanopubs += qs.get("np").toString)
        println(s"Fetched ${nanopubs.size} nanopublication URIs from ${preferredEndpoint._2}")
        done = true
      } catch {
        case e: Exception =>
          println(s"Failed to fetch nanopublication URIs from ${preferredEndpoint._2}: $e")
          try {
            cycleNextEndpoint()
          } catch {
            case e: Exception =>
              println(s"Failed to fetch nanopublication URIs from all endpoints: $e")
              done = true
          }
      }
    nanopubs.toSeq

  private def execQuery(query: String, endpoint: String, callback: QuerySolution => Unit): Unit =
    Using.resource(RDFConnection.connect(endpoint)) { conn =>
      conn.querySelect(query, qs => callback(qs))
    }

  private def cycleNextEndpoint(): Unit =
    val (index, url) = preferredEndpoint
    val nextIndex = index + 1
    if nextIndex >= endpoints.length then
      throw new RuntimeException("No more SPARQL endpoints to try -- can't fetch nanopublications")
    println(s"SPARQL endpoint $url failed, trying next one: ${endpoints(nextIndex)}")
    preferredEndpoint = (nextIndex, endpoints(nextIndex))

  private def parametrizeQuery(query: String, params: Map[String, String]): String =
    params.foldLeft(query) { case (q, (k, v)) =>
      q.replace(s"<<$k>>", v)
    }
