package io.github.riverbench.ci_worker
package util.releases

import util.PurlMaker
import io.github.riverbench.ci_worker.util.rdf.RdfUtil

import org.apache.jena.rdf.model.Resource
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

object PreviousVersionHelper:
  private def add(resource: Resource, tag: String): Unit =
    val prevVersionPurl = PurlMaker.unMake(resource.getURI).get.copy(version = tag.drop(1))
    val prevUrl = prevVersionPurl.getUrl
    resource.addProperty(RdfUtil.dcatPreviousVersion, resource.getModel.createResource(prevUrl))
    println(s"Added previous version info for ${prevVersionPurl.kind}/${prevVersionPurl.id}: $prevUrl")

  def addPreviousVersionInfo(resource: Resource, repo: String)(using as: ActorSystem[_]): Future[Unit] =
    given ExecutionContext = as.executionContext
    ReleaseInfoApi.getLatestReleaseInfo(repo) map {
      case Some(info) if info.tag_name.startsWith("v") => add(resource, info.tag_name)
      case _ => println(s"No release info found for $repo, skipping adding previous version info")
    }

  def addPreviousVersionInfoSynchronous(resource: Resource, repo: String)(using as: ActorSystem[_]): Unit =
    Await.result(addPreviousVersionInfo(resource, repo), 20.seconds)

  def addPreviousVersionInfoPreloaded(maybeInfo: Option[ReleaseInfo], resource: Resource,
                                      filename: Option[String]): Unit =
    maybeInfo match
      case Some(info) if info.tag_name.startsWith("v") => filename match
        case Some(f) if info.assets.exists(_.name == f) => add(resource, info.tag_name)
        case None => add(resource, info.tag_name)
        case _ => println(s"No previous version asset found for $filename, this must be a new item in this release")
      case _ => ()
