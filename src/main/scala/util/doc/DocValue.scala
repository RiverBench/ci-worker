package io.github.riverbench.ci_worker
package util.doc

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model

object DocValue:
  def apply(value: model.RDFNode): DocValue = value match
    case literal: model.Literal => Literal(literal)
    case _ => Text(value.toString)

  case class Text(value: String) extends DocValue:
    def toMarkdown: String = value.strip

  case class Literal(value: model.Literal) extends DocValue:
    def toMarkdown: String = value.getDatatype match
      case XSDboolean => if value.getBoolean then "yes" else "no"
      case _ => value.getLexicalForm.strip

  case class List(values: Iterable[DocValue]) extends DocValue:
    def toMarkdown: String = "\n  - " + values.map(_.toMarkdown).mkString("\n  - ")

trait DocValue:
  def toMarkdown: String

