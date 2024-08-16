package io.github.riverbench.ci_worker
package util.external

import util.io.HttpHelper

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.model.{MediaRange, MediaType}

import java.nio.file.{Files, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future}
import _root_.io.circe.syntax.*
import _root_.io.circe.generic.auto.*
import _root_.io.circe.parser.decode

object DoiBibliography:
  case class BibliographyEntry(doi: String, bib: String)
  case class BibliographyCache(entries: List[BibliographyEntry])

  private val workingCache = collection.concurrent.TrieMap.empty[String, String]
  private val usedDois = collection.concurrent.TrieMap.empty[String, String]
  private val mediaBibliography = MediaRange(MediaType.text("x-bibliography", "style=apa"))

  // Initialization code
  {
    val cacheFile = CiFileCache.getCachedFile("bibliography", "doi-cache.json")
    if !cacheFile.exists() then
      collection.concurrent.TrieMap.empty[String, String]
    else
      val json = Files.readString(cacheFile.toPath)
      val cache = decode[BibliographyCache](json).fold(err => {
        println(s"Failed to parse bibliography cache: $err")
        BibliographyCache(Nil)
      }, identity)
      workingCache ++= cache.entries.map(e => e.doi -> e.bib)
      println(s"Loaded bibliography cache with ${workingCache.size} entries from $cacheFile")
  }

  def getBibliography(doiLike: String)(using as: ActorSystem[_]): Future[String] =
    given ExecutionContext = as.executionContext
    extractDoi(doiLike) flatMap { doi =>
      usedDois.put(doi, doi)
      workingCache.get(doi) match
        case Some(bib) => Future.successful(bib)
        case None =>
          val bib = fetchBibliography(doi)
          bib.foreach(b => {
            println(s"Fetched bibliography for $doi")
            workingCache.put(doi, b)
          })
          bib
    }

  def saveCache(): Unit =
    val cacheFile = CiFileCache.getCachedFile("bibliography", "doi-cache.json")
    val cache = BibliographyCache(
      workingCache
        .filter(e => usedDois.contains(e._1))
        .map(e => BibliographyEntry(e._1, e._2)).toList
    )
    Files.writeString(cacheFile.toPath, cache.asJson.spaces2, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    println(s"Saved bibliography cache with ${cache.entries.size} entries to $cacheFile")

  private def extractDoi(doiLike: String): Future[String] =
    val split = doiLike.split("/")
    if split.length < 2 then
      Future.failed(new IllegalArgumentException(s"Invalid DOI-like string: $doiLike"))
    else
      Future.successful(split(split.length - 2) + "/" + split.last)

  private def fetchBibliography(doi: String)(using as: ActorSystem[_]): Future[String] =
    given ExecutionContext = as.executionContext
    val url = s"https://doi.org/$doi"
    HttpHelper.getWithFollowRedirects(url, accept = Some(mediaBibliography)) flatMap { response =>
      if response.status.isSuccess then
        response.entity.contentType.mediaType.subType match
          case "x-bibliography" =>
            response.entity.dataBytes.runReduce(_ ++ _).map(_.utf8String)
          case _ => Future.failed(
            RuntimeException(f"Expected x-bibliography, got ${response.entity.contentType.mediaType}")
          )
      else
        Future.failed(RuntimeException(f"Failed to fetch bibliography for $doi: ${response.status}"))
    }

