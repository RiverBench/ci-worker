package io.github.riverbench.ci_worker
package util

import scala.util.matching.Regex

object PurlMaker:
  private val purlPattern = (f"""^${Regex.quote(AppConfig.CiWorker.rbRootUrl)}v/([a-z0-9.]+)/""" +
    """(categories|tasks|datasets|profiles)/([a-z0-9-]+)(#.*)?""").r

  def category(id: String, version: String): String = inner(id, version, "categories")

  def task(id: String, version: String): String = inner(id, version, "tasks")
  
  def profile(id: String, version: String): String = inner(id, version, "profiles")
  
  private inline def inner(id: String, version: String, kind: String): String =
    f"${AppConfig.CiWorker.rbRootUrl}v/$version/$kind/$id"
  
  // TODO: after introducing the remaining makers, update DocValue.InternalLink to use this

  case class Purl(id: String, version: String, kind: String)

  def unMake(purl: String): Option[Purl] =
    purl match
      case purlPattern(version, kind, id, _) => Some(Purl(id, version, kind))
      case _ => None
