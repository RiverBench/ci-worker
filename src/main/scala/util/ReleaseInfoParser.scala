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
    parse(path).toOption.get.assets
      .find(_.name == "source.tar.gz")
      .get.browser_download_url
