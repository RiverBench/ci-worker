package io.github.riverbench.ci_worker
package util.releases

import _root_.io.circe.*
import _root_.io.circe.parser.decode

import java.nio.file.{Files, Path}

object ReleaseInfoParser:
  def parse(path: Path): Either[Error, ReleaseInfo] =
    val json = Files.readString(path)
    parse(json)

  def parse(json: String): Either[Error, ReleaseInfo] =
    decode[ReleaseInfo](json)

  def getDatasetUrl(path: Path): String =
    parse(path).toOption.get.assets
      .find(_.name == "source.tar.gz")
      .get.browser_download_url
