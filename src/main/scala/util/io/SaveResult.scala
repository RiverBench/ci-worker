package io.github.riverbench.ci_worker
package util.io

import akka.stream.IOResult
import util.{MetadataInfo, RdfUtil}

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.RDF

case class SaveResult(io: IOResult, size: Long, md5: String, sha1: String):
  def addToRdf(distRes: Resource, mi: MetadataInfo, flat: Boolean): Unit =
    distRes.addProperty(RdfUtil.dcatByteSize, size.toString, XSDinteger)
    distRes.addProperty(RdfUtil.dcatCompressFormat, "application/gzip")

    if flat then
      distRes.addProperty(RdfUtil.dcatPackageFormat, "application/tar")
      if mi.elementType == "triples" then
        distRes.addProperty(RdfUtil.dcatMediaType, "text/turtle")
      else
        distRes.addProperty(RdfUtil.dcatMediaType, "application/trig")
    else
      if mi.elementType == "triples" then
        distRes.addProperty(RdfUtil.dcatMediaType, "application/n-triples")
      else
        distRes.addProperty(RdfUtil.dcatMediaType, "application/n-quads")

    val md5Checksum = distRes.getModel.createResource()
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmMd5)
      .addProperty(RdfUtil.spdxChecksumValue, md5)
    distRes.addProperty(RdfUtil.spdxChecksum, md5Checksum)

    val sha1Checksum = distRes.getModel.createResource()
      .addProperty(RDF.`type`, RdfUtil.SpdxChecksum)
      .addProperty(RdfUtil.spdxAlgorithm, RdfUtil.spdxChecksumAlgorithmSha1)
      .addProperty(RdfUtil.spdxChecksumValue, sha1)
    distRes.addProperty(RdfUtil.spdxChecksum, sha1Checksum)
