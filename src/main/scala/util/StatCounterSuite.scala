package io.github.riverbench.ci_worker
package util

import org.apache.jena.query.Dataset
import org.apache.jena.sparql.core.DatasetGraph

import scala.::
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object StatCounterSuite:
  case class Result(iris: StatCounter.Result, blankNodes: StatCounter.Result, literals: StatCounter.Result,
                    simpleLiterals: StatCounter.Result, dtLiterals: StatCounter.Result,
                    langLiterals: StatCounter.Result, quotedTriples: StatCounter.Result,
                    subjects: StatCounter.Result, predicates: StatCounter.Result,
                    objects: StatCounter.Result, graphs: StatCounter.Result,
                    statements: StatCounter.Result)

class StatCounterSuite(val size: Long):
  import StatCounter.*

  private val cIris = new StatCounter[String](size)
  private val cBlankNodes = new StatCounter[String](size)
  private val cLiterals = new StatCounter[String](size)
  private val cSimpleLiterals = new StatCounter[String](size)
  private val cDtLiterals = new StatCounter[String](size)
  private val cLangLiterals = new StatCounter[String](size)

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
    val simpleLiterals = mutable.Set[String]()
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
          simpleLiterals += n.getLiteralLexicalForm
      else if n.isNodeTriple then
        // isomorphism in RDF-star is a pain...
        quotedTripleCount += 1
    })

    cIris.addUnique(iris)
    cBlankNodes.addUnique(blankNodes)
    cLiterals.addUnique(literals)
    cSimpleLiterals.addUnique(simpleLiterals)
    cDtLiterals.addUnique(dtLiterals)
    cLangLiterals.addUnique(langLiterals)

    cQuotedTriples.lightAdd(quotedTripleCount)

    cSubjects.addUnique(subjects)
    cPredicates.addUnique(predicates)
    cObjects.addUnique(objects)
    cStatements.lightAdd(stCount)

  def result: StatCounterSuite.Result =
    StatCounterSuite.Result(cIris.result, cBlankNodes.result, cLiterals.result, cSimpleLiterals.result,
      cDtLiterals.result, cLangLiterals.result, cQuotedTriples.result, cSubjects.result,
      cPredicates.result, cObjects.result, cGraphs.result, cStatements.result)