package io.github.riverbench.ci_worker
package util.io

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Location

import scala.concurrent.{ExecutionContext, Future}

object HttpHelper:
  def getWithFollowRedirects(url: String, n: Int = 0)(implicit as: ActorSystem[_]): Future[HttpResponse] =
    given ExecutionContext = as.executionContext
    if n > 10 then
      Future.failed(new RuntimeException(s"Too many redirects for $url"))
    else
      Http().singleRequest(HttpRequest(uri = url))
        .flatMap {
          // Follow redirects
          case HttpResponse(StatusCodes.Redirection(_), headers, _, _) =>
            val newUri = headers.collect { case Location(loc) => loc }.head
            getWithFollowRedirects(newUri.toString, n + 1)
          case r => Future { r }
        }
