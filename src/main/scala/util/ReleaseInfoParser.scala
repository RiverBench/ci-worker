package io.github.riverbench.ci_worker
package util

import _root_.io.circe.*
import _root_.io.circe.generic.semiauto.*
import _root_.io.circe.parser.decode

import java.nio.file.{Files, Path}

object ReleaseInfoParser:
  case class ReleaseInfo(assets: List[Asset])
  case class Asset(name: String, browser_download_url: String)

  implicit val relInfoDecoder: Decoder[ReleaseInfo] = deriveDecoder[ReleaseInfo]
  implicit val assetDecoder: Decoder[Asset] = deriveDecoder[Asset]

  def parse(path: Path): Either[Error, ReleaseInfo] =
    val json = Files.readString(path)
    decode[ReleaseInfo](json)

  def getDatasetUrl(path: Path): String =
    val possibleNames = List("triples.tar.gz", "quads.tar.gz", "graphs.tar.gz")
    parse(path).toOption.get.assets
      .filter(asset => possibleNames.contains(asset.name))
      .head.browser_download_url
