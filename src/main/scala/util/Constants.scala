package io.github.riverbench.ci_worker
package util

import org.apache.jena.riot.RDFFormat

object Constants:
  val outputFormats = Seq(
    ("ttl", RDFFormat.TURTLE_PRETTY),
    ("rdf", RDFFormat.RDFXML_PRETTY),
    ("nt", RDFFormat.NTRIPLES_UTF8),
  )

  val packageSizes = Seq[Long](
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
  )

  def packageSizeToHuman(size: Long, showAsFull: Boolean = false) = size match
    case 10_000 => "10K"
    case 100_000 => "100K"
    case 1_000_000 => "1M"
    case 10_000_000 => "10M"
    case 100_000_000 => "100M"
    case 1_000_000_000 => "1B"
    case _ => if showAsFull then "Full" else size.toString
