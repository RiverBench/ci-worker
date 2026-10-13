package io.github.riverbench.ci_worker
package util.io

import commands.PackageCommand.DistType
import util.rdf.{ElementType, MetadataInfo, RdfUtil}

import eu.neverblink.jelly.core.JellyConstants
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.RDF
import org.apache.pekko.stream.IOResult

case class SaveResult(io: IOResult, name: String, size: Long, md5: String, sha1: String):
  /**
   * Local identifier of the distribution, e.g., "flat-10k" for "flat_10K.nt.gz".
   * Jelly-RDF 1.2 files ("jelly_10K.2.jelly.gz") get the "jelly12" prefix to not clash with Jelly-RDF 1.1 files.
   */
  def getLocalId(dType: DistType): String =
    val id = name.split('.').head.toLowerCase.replace('_', '-')
    if dType == DistType.Jelly12 then id.replaceFirst("^jelly-", "jelly12-") else id

  def addToRdf(distRes: Resource, mi: MetadataInfo, dType: DistType): Unit =
    distRes.addLiteral(RdfUtil.dcatByteSize, size)
    distRes.addProperty(RdfUtil.dcatCompressFormat, "application/gzip")
    distRes.addProperty(RdfUtil.dctermsIdentifier, getLocalId(dType))

    dType match
      case DistType.Flat =>
        // Flat files using the RDF Message Log format:
        // https://w3c-cg.github.io/rsp/spec/messages#turtle
        if mi.streamTypes.exists(_.elementType == ElementType.Triple) then
          distRes.addProperty(RdfUtil.dcatMediaType, "application/n-triples")
        else
          distRes.addProperty(RdfUtil.dcatMediaType, "application/n-quads")
      case DistType.Stream =>
        distRes.addProperty(RdfUtil.dcatPackageFormat, "application/tar")
        if mi.streamTypes.exists(_.elementType == ElementType.Triple) then
          distRes.addProperty(RdfUtil.dcatMediaType, "text/turtle")
        else
          distRes.addProperty(RdfUtil.dcatMediaType, "application/trig")
      case DistType.Jelly | DistType.Jelly12 =>
        distRes.addProperty(RdfUtil.dcatMediaType, JellyConstants.JELLY_CONTENT_TYPE)

    val md5Checksum = distRes.getModel.createResource(RdfUtil.newAnonId(md5.getBytes))
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmMd5)
      .addProperty(RdfUtil.spdxChecksumValue, md5)
      .addLiteral(RdfUtil.hasDocWeight, 1)
    distRes.addProperty(RdfUtil.spdxChecksum, md5Checksum)

    val sha1Checksum = distRes.getModel.createResource(RdfUtil.newAnonId(sha1.getBytes))
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmSha1)
      .addProperty(RdfUtil.spdxChecksumValue, sha1)
      .addLiteral(RdfUtil.hasDocWeight, 2)
    distRes.addProperty(RdfUtil.spdxChecksum, sha1Checksum)
