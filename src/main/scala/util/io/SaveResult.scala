package io.github.riverbench.ci_worker
package util.io

import commands.PackageCommand.DistType
import util.{ElementType, MetadataInfo, RdfUtil}

import eu.ostrzyciel.jelly.core
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.RDF
import org.apache.pekko.stream.IOResult

case class SaveResult(io: IOResult, name: String, size: Long, md5: String, sha1: String):
  def getLocalId: String = name.split('.').head.toLowerCase.replace('_', '-')

  def addToRdf(distRes: Resource, mi: MetadataInfo, dType: DistType): Unit =
    distRes.addProperty(RdfUtil.dcatByteSize, size.toString, XSDinteger)
    distRes.addProperty(RdfUtil.dcatCompressFormat, "application/gzip")
    distRes.addProperty(RdfUtil.dctermsIdentifier, getLocalId)

    dType match
      case DistType.Flat =>
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
      case DistType.Jelly =>
        distRes.addProperty(RdfUtil.dcatMediaType, core.Constants.jellyContentType)

    val md5Checksum = distRes.getModel.createResource()
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmMd5)
      .addProperty(RdfUtil.spdxChecksumValue, md5)
      .addProperty(RdfUtil.hasDocWeight, "1", XSDinteger)
    distRes.addProperty(RdfUtil.spdxChecksum, md5Checksum)

    val sha1Checksum = distRes.getModel.createResource()
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmSha1)
      .addProperty(RdfUtil.spdxChecksumValue, sha1)
      .addProperty(RdfUtil.hasDocWeight, "2", XSDinteger)
    distRes.addProperty(RdfUtil.spdxChecksum, sha1Checksum)
