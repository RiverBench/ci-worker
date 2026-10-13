package io.github.riverbench.ci_worker
package util

import eu.neverblink.jelly.convert.jena.riot.*
import eu.neverblink.jelly.core.{JellyConstants, JellyOptions}
import org.apache.jena.riot.{RDFFormat, RDFWriterRegistry}

object Constants:
  // Register Jelly language in Apache Jena
  JellyLanguage.register()

  /**
   * Jelly-RDF 1.1 format, published in .jelly files.
   * Jena writes Jelly-RDF 1.2 by default (JellyFormat.JELLY_BIG_STRICT), published in .2.jelly files.
   */
  val jelly11Format = RDFFormat(
    JellyLanguage.JELLY,
    JellyFormatVariant.builder()
      .options(JellyOptions.BIG_STRICT.clone().setVersion(JellyConstants.PROTO_VERSION_1_1_X))
      .build()
  )
  RDFWriterRegistry.register(jelly11Format, JellyGraphWriterFactory())
  RDFWriterRegistry.register(jelly11Format, JellyDatasetWriterFactory())

  val jelly12Format = JellyFormat.JELLY_BIG_STRICT

  val outputFormats = Seq(
    ("ttl", RDFFormat.TURTLE_PRETTY),
    ("rdf", RDFFormat.RDFXML_PRETTY),
    ("nt", RDFFormat.NTRIPLES_UTF8),
    ("jelly", jelly11Format),
    ("2.jelly", jelly12Format),
  )

  val datasetOutputFormats = Seq(
    ("trig", RDFFormat.TRIG_PRETTY),
    ("nq", RDFFormat.NQUADS_UTF8),
    ("jelly", jelly11Format),
    ("2.jelly", jelly12Format),
  )

  val allowedDocExtensions = Set("md", "jpg", "png", "svg", "jpeg", "bmp", "webp", "gif")

  val packageSizes = Seq[Long](
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
  )

  val streamSamples = Seq[Long](0, 10, 100, 1000, 10_000)

  def packageSizeToHuman(size: Long, showAsFull: Boolean = false) = size match
    case 10_000 => "10K"
    case 100_000 => "100K"
    case 1_000_000 => "1M"
    case 10_000_000 => "10M"
    case 100_000_000 => "100M"
    case 1_000_000_000 => "1B"
    case _ => if showAsFull then "Full" else size.toString

  val taxonomyDocBaseLink = "https://w3id.org/stax/dev/taxonomy#"
  val baseRepoUrl = "https://github.com/RiverBench"
