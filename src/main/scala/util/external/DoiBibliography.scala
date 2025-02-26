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
import org.apache.jena.rdf.model.Model

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object DoiBibliography:
  case class BibliographyEntry(doi: String, apa: String, bibtex: String)
  case class BibliographyCache(entries: List[BibliographyEntry])

  private val workingCache = collection.concurrent.TrieMap.empty[String, BibliographyEntry]
  private val usedDois = collection.concurrent.TrieMap.empty[String, String]
  private val mediaBibliographyApa = MediaType.text("x-bibliography", "style=apa")
  private val mediaBibtex = MediaType.applicationWithOpenCharset("x-bibtex")

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
      workingCache ++= cache.entries.map(e => e.doi -> e)
      println(s"Loaded bibliography cache with ${workingCache.size} entries from $cacheFile")
  }

  def isDoiLike(s: String): Boolean =
    s != null && (s.contains("://doi.org") || s.contains("://dx.doi.org"))

  def getPotentialDois(inModels: IterableOnce[Model]): Seq[String] =
    inModels.iterator
      .flatMap(_.listObjects().asScala)
      .filter(_.isResource)
      .map(_.asResource().getURI)
      .filter(isDoiLike)
      .toSeq

  def preloadBibliography(doiLikes: Seq[String])(using as: ActorSystem[?]): Future[Unit] =
    given ExecutionContext = as.executionContext
    Future.sequence(doiLikes.map(getBibliography)) map (_ => {
      saveCache()
    })

  def getBibliography(doiLike: String)(using as: ActorSystem[?]): Future[BibliographyEntry] =
    given ExecutionContext = as.executionContext
    Future.fromTry(extractDoi(doiLike)) flatMap { doi =>
      usedDois.put(doi, doi)
      workingCache.get(doi) match
        case Some(entry) => Future.successful(entry)
        case None =>
          for
            apa <- fetchBibliography(doi, mediaBibliographyApa)
            bibtex <- fetchBibliography(doi, mediaBibtex)
          yield
            println(s"Fetched bibliography for $doi")
            val entry = BibliographyEntry(doi, apa, bibtex)
            workingCache.put(doi, entry)
            entry
    }

  def getBibliographyFromCache(doiLike: String): Option[BibliographyEntry] =
    extractDoi(doiLike).toOption.flatMap(doi => workingCache.get(doi))

  def saveCache(): Unit =
    val cacheFile = CiFileCache.getCachedFile("bibliography", "doi-cache.json")
    val cache = BibliographyCache(
      workingCache.filter(e => usedDois.contains(e._1)).values.toList
    )
    Files.writeString(cacheFile.toPath, cache.asJson.spaces2, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    println(s"Saved bibliography cache with ${cache.entries.size} entries to $cacheFile")

  private def extractDoi(doiLike: String): Try[String] =
    val split = doiLike.split("/")
    if split.length < 2 then
      Failure(new IllegalArgumentException(s"Invalid DOI-like string: $doiLike"))
    else
      Success(split(split.length - 2) + "/" + split.last)

  private def fetchBibliography(doi: String, mediaType: MediaType)(using as: ActorSystem[?]): Future[String] =
    given ExecutionContext = as.executionContext
    val url = s"https://doi.org/$doi"
    HttpHelper.getWithFollowRedirects(url, accept = Some(MediaRange(mediaType))) flatMap { response =>
      if response.status.isSuccess then
        response.entity.contentType.mediaType.subType match
          case mediaType.subType =>
            response.entity.dataBytes.runReduce(_ ++ _).map(_.utf8String.trim)
          case _ => Future.failed(
            RuntimeException(f"Expected $mediaType, got ${response.entity.contentType.mediaType}")
          )
      else
        Future.failed(RuntimeException(f"Failed to fetch bibliography for $doi with $mediaType: ${response.status}"))
    }

