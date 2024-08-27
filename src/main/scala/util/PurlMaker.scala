package io.github.riverbench.ci_worker
package util

import scala.util.matching.Regex

object PurlMaker:
  private val purlPattern = (f"""^${Regex.quote(AppConfig.CiWorker.rbRootUrl)}v/([a-z0-9.]+)/""" +
    """(categories|tasks|profiles)/([a-z0-9-]+)(#.*)?""").r
  
  private val datasetPurlPattern = 
    f"""^${Regex.quote(AppConfig.CiWorker.rbRootUrl)}datasets/([a-z0-9-]+)/([a-z0-9.]+)(#.*)?""".r

  def category(id: String, version: String, subpage: Option[String] = None): String =
    inner(id, version, "categories", subpage)

  def task(id: String, version: String, subpage: Option[String] = None): String = 
    inner(id, version, "tasks", subpage)
  
  def profile(id: String, version: String, subpage: Option[String] = None): String = 
    inner(id, version, "profiles", subpage)

  def dataset(id: String, version: String): String =
    f"${AppConfig.CiWorker.rbRootUrl}datasets/$id/$version"
  
  private inline def inner(id: String, version: String, kind: String, subpage: Option[String]): String =
    val s = f"${AppConfig.CiWorker.rbRootUrl}v/$version/$kind/$id"
    subpage.fold(s)(sp => s"$s/$sp")
  
  // TODO: after introducing the remaining makers, update DocValue.InternalLink to use this

  case class Purl(id: String, version: String, kind: String):
    def getUrl: String = kind match
      case "datasets" => dataset(id, version)
      case _ => inner(id, version, kind, None)

  def unMake(purl: String): Option[Purl] =
    purl match
      case purlPattern(version, kind, id, _) => Some(Purl(id, version, kind))
      case datasetPurlPattern(id, version, _) => Some(Purl(id, version, "datasets"))
      case _ => None
