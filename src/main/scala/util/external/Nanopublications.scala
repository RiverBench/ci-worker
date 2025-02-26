package io.github.riverbench.ci_worker
package util.external

import util.io.HttpHelper

import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParser}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.model.{MediaRange, MediaType}
import org.apache.pekko.stream.scaladsl.StreamConverters

import java.nio.file.{Files, StandardOpenOption}
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}

object Nanopublications:
  private val mediaTrig = MediaRange(MediaType.applicationWithOpenCharset("trig"))
  private val workingCache = collection.concurrent.TrieMap.empty[String, Dataset]
  private val usedUris = collection.concurrent.TrieMap.empty[String, String]

  // Initialization code
  {
    val cachedFiles = CiFileCache.getCachedFilesInDir("nanopubs")
    for file <- cachedFiles do
      val fileName = file.getName
      val nanopub = RDFDataMgr.loadDataset(file.toURI.toString, Lang.TRIG)
      workingCache.put(fileNameToUri(fileName), nanopub)
    println(s"Loaded ${workingCache.size} nanopublications from cache")
  }

  def getNanopublication(uri: String)(using as: ActorSystem[?]): Future[Dataset] =
    given ExecutionContext = as.executionContext
    usedUris.put(uri, uri)
    workingCache.get(uri) match
      case Some(ds) => Future.successful(ds)
      case None =>
        val dsFuture = fetchNanopublication(uri)
        dsFuture.foreach(ds => {
          println(s"Fetched nanopublication $uri")
          workingCache.put(uri, ds)
        })
        dsFuture

  def saveCache(): Unit =
    CiFileCache.deleteOldCachedFiles(
      "nanopubs",
      keep = usedUris.keys.map(uriToFileName)
    )

    for uri <- usedUris.keys do
      val file = CiFileCache.getCachedFile("nanopubs", uriToFileName(uri))
      if !file.exists() then
        val os = Files.newOutputStream(file.toPath, StandardOpenOption.CREATE_NEW)
        RDFDataMgr.write(
          os,
          workingCache(uri),
          Lang.TRIG
        )
        os.close()
        println(s"Saved nanopublication $uri to $file")

  private def uriToFileName(uri: String): String =
    Base64.getUrlEncoder.encodeToString(uri.getBytes) + ".trig"

  private def fileNameToUri(fileName: String): String =
    new String(Base64.getUrlDecoder.decode(fileName.stripSuffix(".trig")))

  private def fetchNanopublication(uri: String)(using as: ActorSystem[?]): Future[Dataset] =
    given ExecutionContext = as.executionContext
    HttpHelper.getWithFollowRedirects(uri, accept = Some(mediaTrig)) flatMap { response =>
      if response.status.isSuccess then
        response.entity.contentType.mediaType.subType match
          case "trig" =>
            val is = response.entity.dataBytes.runWith(StreamConverters.asInputStream())
            val ds = DatasetFactory.create()
            RDFParser.source(is).lang(Lang.TRIG).parse(ds)
            Future.successful(ds)
          case _ => Future.failed(
            RuntimeException(f"Expected application/trig, got ${response.entity.contentType.mediaType}")
          )
      else
        Future.failed(RuntimeException(f"Failed to fetch nanopub for $uri: ${response.status}"))
    }
