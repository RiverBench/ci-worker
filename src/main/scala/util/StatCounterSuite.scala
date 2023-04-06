package io.github.riverbench.ci_worker
package util

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.query.Dataset
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.jena.vocabulary.RDF

import scala.::
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object StatCounterSuite:
  case class Result(iris: StatCounter.Result, blankNodes: StatCounter.Result, literals: StatCounter.Result,
                    plainLiterals: StatCounter.Result, dtLiterals: StatCounter.Result,
                    langLiterals: StatCounter.Result, quotedTriples: StatCounter.Result,
                    subjects: StatCounter.Result, predicates: StatCounter.Result,
                    objects: StatCounter.Result, graphs: StatCounter.Result,
                    statements: StatCounter.Result):

    def addToRdf(distRes: Resource, mi: MetadataInfo, size: Long, flat: Boolean):
    Unit =
      val m = distRes.getModel
      val toAdd = Seq(
        "IriCountStatistics" -> iris,
        "BlankNodeCountStatistics" -> blankNodes,
        "LiteralCountStatistics" -> literals,
        "PlainLiteralCountStatistics" -> plainLiterals,
        "DatatypeLiteralCountStatistics" -> dtLiterals,
        "LanguageLiteralCountStatistics" -> langLiterals,
        "QuotedTripleCountStatistics" -> quotedTriples,
        "SubjectCountStatistics" -> subjects,
        "PredicateCountStatistics" -> predicates,
        "ObjectCountStatistics" -> objects,
        "GraphCountStatistics" -> graphs,
        "StatementCountStatistics" -> statements
      )

      // Info about the distribution
      // Media type, byte size, link, checksum are added separately by the packaging pipeline
      distRes.addProperty(RDF.`type`, RdfUtil.Distribution)
      distRes.addProperty(RDF.`type`, RdfUtil.DcatDistribution)

      val sizeString = if size == mi.elementCount then
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.fullDistribution)
        "Full"
      else
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.partialDistribution)
        Constants.packageSizeToHuman(size) + " elements"

      if flat then
        distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.flatDistribution)
        distRes.addProperty(RdfUtil.dcatTitle, s"$sizeString flat distribution")
      else
        mi.elementType match
          case "graphs" =>
            distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.graphStreamDistribution)
            distRes.addProperty(RdfUtil.dcatTitle, s"$sizeString graph stream distribution")
          case "triples" =>
            distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.tripleStreamDistribution)
            distRes.addProperty(RdfUtil.dcatTitle, s"$sizeString triple stream distribution")
          case "quads" =>
            distRes.addProperty(RdfUtil.hasDistributionType, RdfUtil.quadStreamDistribution)
            distRes.addProperty(RdfUtil.dcatTitle, s"$sizeString quad stream distribution")

      distRes.addProperty(RdfUtil.hasStreamElementCount, size.toString, XSDinteger)

      // Stats
      toAdd.foreach((name, stat) => {
        val statRes = m.createResource()
        statRes.addProperty(RDF.`type`, m.createResource(RdfUtil.pRb + name))
        stat.addToRdf(statRes)
        distRes.addProperty(RdfUtil.hasStatistics, statRes)
      })

/**
 * A set of stat counters for a stream.
 * @param size expected size of the stream – used to allocate bloom filters
 */
class StatCounterSuite(val size: Long):
  import StatCounter.*

  // A bad heuristic: 10x the size of the stream is assumed to be the number of elements in the bloom filters
  private val cIris = new StatCounter[String](10 * size)
  private val cLiterals = new StatCounter[String](10 * size)
  private val cPlainLiterals = new StatCounter[String](10 * size)
  private val cDtLiterals = new StatCounter[String](10 * size)
  private val cLangLiterals = new StatCounter[String](10 * size)

  private val cBlankNodes = new LightStatCounter[String]()
  private val cQuotedTriples = new LightStatCounter[String]()

  private val cSubjects = new LightStatCounter[String]()
  private val cPredicates = new LightStatCounter[String]()
  private val cObjects = new LightStatCounter[String]()
  private val cGraphs = new LightStatCounter[String]()

  private val cStatements = new LightStatCounter[String]()

  def add(ds: DatasetGraph): Unit =
    cGraphs.lightAdd(ds.listGraphNodes().asScala.size)
    val subjects = mutable.Set[String]()
    val predicates = mutable.Set[String]()
    val objects = mutable.Set[String]()
    val iris = mutable.Set[String]()
    val blankNodes = mutable.Set[String]()
    val literals = mutable.Set[String]()
    val plainliterals = mutable.Set[String]()
    val dtLiterals = mutable.Set[String]()
    val langLiterals = mutable.Set[String]()
    var quotedTripleCount = 0
    var stCount = 0

    val allNodes = ds.find().asScala.flatMap(t => {
      subjects += t.getSubject.toString(false)
      predicates += t.getPredicate.toString(false)
      objects += t.getObject.toString(false)
      stCount += 1

      t.getSubject :: t.getPredicate :: t.getObject :: Nil
    }) ++ ds.listGraphNodes().asScala

    allNodes.foreach(n => {
      if n.isURI then
        iris += n.getURI
      else if n.isBlank then
        blankNodes += n.getBlankNodeLabel
      else if n.isLiteral then
        val lit = n.toString(false)
        literals += lit
        if n.getLiteralLanguage != "" then
          langLiterals += lit
        else if n.getLiteralDatatypeURI != null then
          dtLiterals += lit
        else
          plainliterals += n.getLiteralLexicalForm
      else if n.isNodeTriple then
        // isomorphism in RDF-star is a pain...
        quotedTripleCount += 1
    })

    cIris.addUnique(iris)
    cBlankNodes.addUnique(blankNodes)
    cLiterals.addUnique(literals)
    cPlainLiterals.addUnique(plainliterals)
    cDtLiterals.addUnique(dtLiterals)
    cLangLiterals.addUnique(langLiterals)

    cQuotedTriples.lightAdd(quotedTripleCount)

    cSubjects.addUnique(subjects)
    cPredicates.addUnique(predicates)
    cObjects.addUnique(objects)
    cStatements.lightAdd(stCount)

  def result: StatCounterSuite.Result =
    StatCounterSuite.Result(cIris.result, cBlankNodes.result, cLiterals.result, cPlainLiterals.result,
      cDtLiterals.result, cLangLiterals.result, cQuotedTriples.result, cSubjects.result,
      cPredicates.result, cObjects.result, cGraphs.result, cStatements.result)