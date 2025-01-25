package io.github.riverbench.ci_worker
package util

import util.rdf.*

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.jena.vocabulary.RDF
import org.apache.pekko.util.ByteString

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object StatCounterSuite:
  case class Result(iris: StatCounter.Result[Long], blankNodes: StatCounter.Result[Long], 
                    literals: StatCounter.Result[Long],
                    plainLiterals: StatCounter.Result[Long], dtLiterals: StatCounter.Result[Long],
                    langLiterals: StatCounter.Result[Long], datatypes: StatCounter.Result[Long],
                    controlChars: StatCounter.Result[Long], quotedTriples: StatCounter.Result[Long],
                    subjects: StatCounter.Result[Long], predicates: StatCounter.Result[Long],
                    objects: StatCounter.Result[Long], graphs: StatCounter.Result[Long],
                    statements: StatCounter.Result[Long], byteDensity: StatCounter.Result[Double]):

    def addToRdf(m: Model, size: Long, totalSize: Long): Resource =
      val toAdd = Seq(
        "IriCountStatistics" -> iris,
        "BlankNodeCountStatistics" -> blankNodes,
        "LiteralCountStatistics" -> literals,
        "SimpleLiteralCountStatistics" -> plainLiterals,
        "DatatypeLiteralCountStatistics" -> dtLiterals,
        "LanguageLiteralCountStatistics" -> langLiterals,
        "DatatypeCountStatistics" -> datatypes,
        "AsciiControlCharacterCountStatistics" -> controlChars,
        "QuotedTripleCountStatistics" -> quotedTriples,
        "SubjectCountStatistics" -> subjects,
        "PredicateCountStatistics" -> predicates,
        "ObjectCountStatistics" -> objects,
        "GraphCountStatistics" -> graphs,
        "StatementCountStatistics" -> statements,
        "ByteDensityStatistics" -> byteDensity
      )
      val sizeName = Constants.packageSizeToHuman(size, true)
      val mainStatRes = m.createResource(RdfUtil.tempDataset.getURI + "#statistics-" + sizeName.toLowerCase)
      mainStatRes.addProperty(RDF.`type`, RdfUtil.StatisticsSet)
      mainStatRes.addProperty(
        RdfUtil.dctermsTitle,
        "Statistics for " + (if sizeName == "Full" then "full" else sizeName) + " distributions"
      )
      val weight = if size == totalSize then 0 else totalSize.toString.length - size.toString.length + 1
      mainStatRes.addLiteral(RdfUtil.hasDocWeight, weight)

      toAdd
        .zipWithIndex
        .foreach((el, i) => {
          val (name, stat) = el
          val statRes = m.createResource(RdfUtil.newAnonId((name + stat.toString).getBytes))
          statRes.addProperty(RDF.`type`, m.createResource(RdfUtil.pRb + name))
          statRes.addLiteral(RdfUtil.hasDocWeight, i)
          stat.addToRdf(statRes)
          mainStatRes.addProperty(RdfUtil.hasStatistics, statRes)
        })
      mainStatRes

/**
 * A set of stat counters for a stream.
 * @param size expected size of the stream – used to allocate bloom filters
 */
class StatCounterSuite(val size: Long):
  import StatCounter.*
  
  // Hack. Jena usually represents the default graph node as null, which is not great for us here.
  private val DEFAULT_GRAPH = NodeFactory.createBlankNode("DEFAULT GRAPH")
  
  private val cIris = new SketchStatCounter()
  private val cLiterals = new SketchStatCounter()
  private val cPlainLiterals = new SketchStatCounter()
  private val cDtLiterals = new SketchStatCounter()
  private val cLangLiterals = new SketchStatCounter()
  private val cDatatypes = new PreciseStatCounter[String]

  private val cAsciiControlChars = LightStatCounter[Char]()
  private val cBlankNodes = new LightStatCounter[String]()
  private val cQuotedTriples = new LightStatCounter[String]()

  private val cSubjects = new SketchStatCounter()
  private val cPredicates = new SketchStatCounter()
  private val cObjects = new SketchStatCounter()
  private val cGraphs = new SketchStatCounter()
  private val cStatements = new LightStatCounter[String]()
  
  private val cByteDensity = new UncountableStatCounter()
  
  def add(ds: DatasetGraph, bytesInFlat: ByteString): Unit =
    if ds.getDefaultGraph.isEmpty then
      cGraphs.add(ds.listGraphNodes().asScala.map(_.toString()).toSeq)
    else
      cGraphs.add((ds.listGraphNodes().asScala.toSeq :+ DEFAULT_GRAPH).map(_.toString()))
      
    val subjects = mutable.Set[Node]()
    val predicates = mutable.Set[Node]()
    val objects = mutable.Set[Node]()
    val iris = mutable.Set[String]()
    val blankNodes = mutable.Set[String]()
    val literals = mutable.Set[String]()
    val simpleLiterals = mutable.Set[String]()
    val dtLiterals = mutable.Set[String]()
    val langLiterals = mutable.Set[String]()
    val datatypes = mutable.Set[String]()
    var controlCharCount = 0
    var quotedTripleCount = 0
    var stCount = 0

    // Find triples recursively – needed for RDF-star
    def getTriples(t: Triple): List[Triple] =
      t +: (t.getSubject :: t.getPredicate :: t.getObject :: Nil)
        .filter(_.isNodeTriple)
        .flatMap(n => getTriples(n.getTriple))

    val allNodes = ds.find().asScala.flatMap(t => {
      stCount += 1
      getTriples(t.asTriple)
    }).flatMap(t => {
      subjects += t.getSubject
      predicates += t.getPredicate
      objects += t.getObject

      t.getSubject :: t.getPredicate :: t.getObject :: Nil
    }) ++ ds.listGraphNodes().asScala
    val usesQuads = ds.listGraphNodes().asScala.size > 1

    allNodes.foreach(n => {
      if n.isURI then
        iris += n.getURI
      else if n.isBlank then
        blankNodes += n.getBlankNodeLabel
      else if n.isLiteral then
        val lit = n.toString()
        controlCharCount += countAsciiControlChars(lit)
        literals += lit
        if n.getLiteralLanguage != "" then
          langLiterals += lit
        else if n.getLiteralDatatypeURI == XSDstring.getURI then
          simpleLiterals += n.getLiteralLexicalForm
        else if n.getLiteralDatatypeURI != null then
          dtLiterals += lit
          datatypes += n.getLiteralDatatypeURI
        else
          simpleLiterals += n.getLiteralLexicalForm
      else if n.isNodeTriple then
        // isomorphism in RDF-star is a pain...
        quotedTripleCount += 1
    })

    cIris.addUnique(iris)
    cBlankNodes.addUnique(blankNodes)
    cLiterals.addUnique(literals)
    cPlainLiterals.addUnique(simpleLiterals)
    cDtLiterals.addUnique(dtLiterals)
    cLangLiterals.addUnique(langLiterals)
    cDatatypes.addUnique(datatypes)
    cAsciiControlChars.lightAdd(controlCharCount)

    cQuotedTriples.lightAdd(quotedTripleCount)

    cSubjects.addUnique(subjects.map(_.toString()))
    cPredicates.addUnique(predicates.map(_.toString()))
    cObjects.addUnique(objects.map(_.toString()))
    cStatements.lightAdd(stCount)
    
    // Note: the byte count includes exactly stCount newlines.
    cByteDensity.addOne(bytesInFlat.length.toDouble / stCount)

  private def countAsciiControlChars(s: String): Int =
    // 0x00–0x1F are disallowed except 0x09 (HT, tab), 0x0A (LF), 0x0D (CR)
    s.count(c => c < 9 || c == 11 || c == 12 || (c > 13 && c < 20))

  def result: StatCounterSuite.Result =
    StatCounterSuite.Result(cIris.result, cBlankNodes.result, cLiterals.result, cPlainLiterals.result,
      cDtLiterals.result, cLangLiterals.result, cDatatypes.result, cAsciiControlChars.result, cQuotedTriples.result,
      cSubjects.result, cPredicates.result, cObjects.result, cGraphs.result, cStatements.result, cByteDensity.result)
