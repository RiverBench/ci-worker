package io.github.riverbench.ci_worker
package util.doc

import util.RdfUtil
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.jdk.CollectionConverters.*

object DocValue:
  case class Text(value: String) extends DocValue:
    def toMarkdown: String = value.strip

  case class Literal(value: model.Literal) extends DocValue:
    def toMarkdown: String = value.getDatatype match
      case XSDboolean => if value.getBoolean then "yes" else "no"
      case _ => value.getLexicalForm.strip

  case class RdfNamedThing(value: model.Resource, ontologies: Model) extends DocValue:
    private val label = RdfUtil.getPrettyLabel(value, Some(ontologies))

    def toMarkdown: String =
      f"$label ([${ontologies.shortForm(value.getURI)}](${value.getURI}))"

    override def getTitle: Option[String] = Some(label)

  case class BlankNode(values: Iterable[(DocProp, DocValue)], name: Option[String]) extends DocValue:
    override val isNestedList = true
    override def toMarkdown: String = values
      .toSeq
      .sortBy(_._1.weight)
      .map { case (prop, value) =>
        val vMd = if value.isNestedList then
          value.toMarkdown.linesIterator.map("  " + _).mkString("\n")
        else value.toMarkdown
        s"\n  - **${prop.toMarkdown}**: $vMd"
      }.mkString

    override def getTitle = name

  case class List(values: Iterable[DocValue], baseName: Option[String]) extends DocValue:
    override val isNestedList = true
    // TODO: sorting by onto-specified prop
    def toMarkdown: String =
      val baseItemName = baseName.getOrElse("Item").strip
      val valuesSorted = values.toSeq.sortBy(_.getTitle.getOrElse(baseItemName))
      val items = for (value, i) <- valuesSorted.zipWithIndex yield
        if value.isNestedList then
          f"  - **${value.getTitle.getOrElse(baseItemName)} (${i + 1})**" +
            value.toMarkdown.linesIterator.map("  " + _).mkString("\n")
        else s"  - ${value.toMarkdown}"
      "\n" + items.mkString("\n")

trait DocValue:
  val isNestedList = false

  def toMarkdown: String

  def getTitle: Option[String] = None
