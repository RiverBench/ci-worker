package io.github.riverbench.ci_worker
package util.releases

import _root_.io.circe.*
import _root_.io.circe.generic.semiauto.*

object ReleaseInfo:
  case class Asset(name: String, browser_download_url: String)
  given assetDecoder: Decoder[Asset] = deriveDecoder[Asset]

case class ReleaseInfo(assets: List[ReleaseInfo.Asset], tag_name: String)

given relInfoDecoder: Decoder[ReleaseInfo] = deriveDecoder[ReleaseInfo]
