package io.github.riverbench.ci_worker
package util.releases

import util.io.HttpHelper

import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

object ReleaseInfoApi:
  def getLatestReleaseInfo(repo: String)(using as: ActorSystem[?]): Future[Option[ReleaseInfo]] =
    given ExecutionContext = as.executionContext
    HttpHelper.getWithFollowRedirects(
      s"https://api.github.com/repos/RiverBench/$repo/releases/latest"
    ) flatMap { response =>
      if response.status.isSuccess then
        response.entity.dataBytes.runReduce(_ ++ _).map(_.utf8String).map(Some(_))
      else if response.status.intValue == 404 then
        Future(None)
      else
        Future.failed(RuntimeException(s"Failed to get latest release info for $repo. Status: ${response.status}"))
    } flatMap {
      case None => Future(None)
      case Some(json) => Future.fromTry(
        ReleaseInfoParser.parse(json).toTry
      ).map(Some(_))
    }
