package io.github.riverbench.ci_worker
package util.doc

import util.{AppConfig, RdfUtil}

import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType
import org.apache.jena.rdf.model
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS}

import scala.jdk.CollectionConverters.*

object DocValue:
  import MarkdownUtil.indent

  case class Text(value: String) extends DocValue:
    def toMarkdown: String = value.strip
    def getSortKey = value.strip

  case class Literal(value: model.Literal) extends DocValue:
    def toMarkdown: String = value.getDatatype match
      case XSDboolean => if value.getBoolean then "yes" else "no"
      case dt: XSDBaseNumericType =>
        val dtName = dt.getURI.toLowerCase
        if dtName.contains("int") || dtName.contains("long") then
          MarkdownUtil.formatInt(value.getLexicalForm)
        else if dtName.contains("float") || dtName.contains("double") || dtName.contains("decimal") then
          MarkdownUtil.formatDouble(value.getLexicalForm)
        else
          value.getLexicalForm.strip
      case _ =>
        val str = value.getLexicalForm.strip
        // Heuristic for hash-like strings
        if !str.contains(' ') && str.length > 10 && str.exists(_.isLetter) && str.exists(_.isDigit) then
          f"`$str`" else str

    def getSortKey = value.getLexicalForm.strip
    
  case class SizeLiteral(value: String) extends DocValue:
    def toMarkdown: String =
      value.strip.toLongOption match
        case Some(long) => MarkdownUtil.formatSize(long)
        case None => value.strip
    def getSortKey = value.strip

  case class RdfNamedThing(value: model.Resource, ontologies: Model) extends DocValue:
    private val label = RdfUtil.getPrettyLabel(value, Some(ontologies))
    private val comment = (
      RdfUtil.getString(value, RDFS.comment, Some(ontologies)) ++
        RdfUtil.getString(value, SKOS.definition, Some(ontologies))
    ).headOption

    def toMarkdown: String =
      f"${MarkdownUtil.toPrettyString(label, comment)} ([${ontologies.shortForm(value.getURI)}](${value.getURI}))"

    override def getTitle: Option[String] = Some(label)

    override def getSortKey: String = label

  object InternalLink:
    def apply(value: model.Resource): InternalLink | Link =
      val uri = value.getURI
      val toSplit = if uri.startsWith(AppConfig.CiWorker.baseDatasetUrl) then
        Some(uri.drop(AppConfig.CiWorker.baseDatasetUrl.length))
      else if uri.startsWith(AppConfig.CiWorker.baseProfileUrl) then
        Some(uri.drop(AppConfig.CiWorker.baseProfileUrl.length))
      else None

      toSplit match
        case Some(toSplit) =>
          val parts = toSplit.split('/')
          if parts.length != 2 then
            Link(value)
          else
            InternalLink(uri, s"${parts(0)} (${parts(1)})")
        case _ => Link(value)

  case class InternalLink(uri: String, name: String) extends DocValue:
    def toMarkdown: String = f"[$name]($uri)"
    def getSortKey = name

  case class Link(value: model.Resource) extends DocValue:
    def toMarkdown: String = f"[${value.getURI}](${value.getURI})"

    override def getSortKey = value.getURI

  case class BlankNode(values: Iterable[(DocProp, DocValue)], name: Option[String]) extends DocValue:
    override val isNestedList = true
    override def toMarkdown: String = values
      .toSeq
      .filter(!_._1.hidden)
      .sortBy(_._1.weight)
      .map { case (prop, value) =>
        val vMd = if value.isNestedList then
          value.toMarkdown.linesIterator.map(indent + _).mkString("\n")
        else value.toMarkdown
        s"\n$indent- **${prop.toMarkdown}**: $vMd"
      }.mkString

    override def getTitle = name

    override def getSortKey = values
      .filter(_._1.prop.getURI == RdfUtil.hasDocWeight.getURI)
      .flatMap((_, dv) => { dv match
        case Literal(lit) => lit.getLexicalForm
          .toIntOption
          .map(i => f"$i%05d")
        case _ => None
      })
      .headOption
      .orElse(name)
      .orElse(values.headOption.map(_._1.label))
      .getOrElse("")

  case class List(values: Iterable[DocValue], baseName: Option[String]) extends DocValue:
    override val isNestedList = true

    def toMarkdown: String =
      val baseItemName = baseName.getOrElse("Item").strip
      val valuesSorted = values.toSeq.sortBy(_.getSortKey)
      val items = for (value, i) <- valuesSorted.zipWithIndex yield
        if value.isNestedList then
          f"$indent- **${value.getTitle.getOrElse(baseItemName)} (${i + 1})**" +
            value.toMarkdown.linesIterator.map(indent + _).mkString("\n")
        else f"$indent- ${value.toMarkdown}"
      "\n" + items.mkString("\n")

    override def getSortKey = baseName.getOrElse("")

trait DocValue:
  val isNestedList = false

  def toMarkdown: String

  def getTitle: Option[String] = None

  def getSortKey: String
