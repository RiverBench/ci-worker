package io.github.riverbench.ci_worker
package util.doc

import util.{AppConfig, PurlMaker, RdfUtil}

import com.ibm.icu.util.ULocale
import io.github.riverbench.ci_worker.util.doc.MarkdownUtil.prettyLabel
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType
import org.apache.jena.rdf.model
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS}

object DocValue:
  import MarkdownUtil.indent

  case class Text(value: String) extends DocValue:
    def toMarkdown: String = value.strip
    def getSortKey = value.strip

  case class Literal(value: model.Literal) extends DocValue:
    def toMarkdown: String =
      if value.getLanguage != "" then
        // Language string: get a human-readable language tag
        val lang = value.getLanguage
        val langName = ULocale.forLanguageTag(value.getLanguage).getDisplayName
        val langNameOption = if langName != "" then Some(langName) else None
        f"${value.getLexicalForm.strip} _(${MarkdownUtil.prettyLabel(lang, langNameOption)})_"
      else value.getDatatype match
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
          // Heuristic for hash-like strings and identifiers
          if !str.contains(' ') && str.length > 10 && str.exists(_.isLetter) && str.exists(_.isDigit) ||
            str.exists(_.isLetter) && str.matches("^[a-z0-9-_.]+$") && str.length > 1 then
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
      f"${MarkdownUtil.prettyLabel(label, comment)} ([${ontologies.shortForm(value.getURI)}](${value.getURI}))"

    override def getTitle: Option[String] = Some(label)

    override def getSortKey: String = label

  object InternalLink:
    def apply(value: model.Resource): InternalLink | Link =
      val uri = value.getURI
      val split = if uri.startsWith(AppConfig.CiWorker.baseDatasetUrl) then
        if uri.contains("#statistics-") then
          val name = uri.split('#').last
          Some(Array(s"#$name", name))
        else
          Some(uri.drop(AppConfig.CiWorker.baseDatasetUrl.length).split('/'))
      else if PurlMaker.unMake(uri).isDefined then
        PurlMaker.unMake(uri).map(p => Array(p.id, p.version))
      else if uri == AppConfig.CiWorker.rbRootUrl then
        Some(Array("RiverBench", "dev"))
      else if uri.startsWith(AppConfig.CiWorker.rbRootUrl + "v/") then
        Some(Array("RiverBench", uri.drop(AppConfig.CiWorker.rbRootUrl.length + 2)))
      else None

      split match
        case Some(parts) =>
          if parts.length != 2 then
            Link(value)
          else if parts(0).startsWith("#") then
            // Anchor link
            InternalLink(parts(0), parts(1))
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

    override def toMarkdownSimple = values.headOption.map(_.toMarkdown).getOrElse("")

    override def getSortKey = baseName.getOrElse("")

  case class Table(values: Map[(String, DocProp), DocValue]) extends DocValue:
    override val noIndent = true

    def toMarkdown: String =
      val sb = new StringBuilder
      val allCols = values
        .toSeq
        .filter { case ((_, col), _) => !col.hidden && col.prop != RDF.`type` }
        .map { case ((_, col), _) => col }
        .distinct
        .sortBy(_.weight)
      sb.append("| ")
      sb.append(allCols.map { col => s"| **${col.toMarkdown}** " }.mkString("") + "|\n")
      sb.append("| --- ")
      sb.append("| --: " * allCols.size + "|\n")

      values
        .toSeq
        .groupBy { case ((row, _), _) => row }
        .toSeq
        // sort by row weight... it's there in the columns
        .sortBy { case (row, cols) =>
          cols
            .filter((k, _) => k._2.prop == RdfUtil.hasDocWeight)
            .map((_, v) => v.getSortKey)
            .headOption
            .getOrElse(row)
        }
        .foreach { case (row, cols) =>
          sb.append(s"| **$row** ")
          for col <- allCols do
            val value = cols.find((k, _) => k._2 == col).map((_, v) => v)
            value match
              case Some(value) => sb.append(s"| ${value.toMarkdownSimple} ")
              case None => sb.append("| _N/A_ ")
          sb.append("|\n")
        }
      sb.toString

    override def getSortKey = ""

trait DocValue:
  val isNestedList = false

  val noIndent = false

  def toMarkdown: String

  /**
   * Markdown without nested lists or anything funny – just return a simple MD
   * string that can be used in other formatting (e.g., a list).
   * @return
   */
  def toMarkdownSimple: String = toMarkdown

  def getTitle: Option[String] = None

  def getSortKey: String
