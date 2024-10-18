package io.github.riverbench.ci_worker
package util.doc

import util.external.DoiBibliography
import util.{AppConfig, PurlMaker, RdfUtil}

import com.ibm.icu.util.ULocale
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType
import org.apache.jena.rdf.model
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{RDF, RDFS, SKOS}

object DocValue:
  import MarkdownUtil.indent

  private val urlRegex = """https?://[^\s]+""".r

  case class Text(value: String) extends DocValue:
    def toMarkdown: String = value.strip
    def getSortKey = value.strip

  case class Literal(value: model.Literal) extends DocValue:
    def toMarkdown: String =
      val lex = Escaping.escapeHtml(value.getLexicalForm.strip())
      if value.getLanguage != "" then
        // Language string: get a human-readable language tag
        val lang = value.getLanguage
        val langName = ULocale.forLanguageTag(lang).getDisplayName
        val langNameOption = if langName != "" then Some(langName) else None
        f"$lex _(${MarkdownUtil.prettyLabelEscaped(lang, langNameOption)})_"
      else value.getDatatype match
        case XSDboolean => if value.getBoolean then "yes" else "no"
        case dt: XSDBaseNumericType =>
          val dtName = dt.getURI.toLowerCase
          if dtName.contains("int") || dtName.contains("long") then
            MarkdownUtil.formatInt(lex)
          else if dtName.contains("float") || dtName.contains("double") || dtName.contains("decimal") then
            MarkdownUtil.formatDouble(lex)
          else
            lex
        case _ =>
          // Heuristic for hash-like strings and identifiers
          if !lex.contains(' ') && lex.length > 10 && lex.exists(_.isLetter) && lex.exists(_.isDigit) ||
            lex.exists(_.isLetter) && lex.matches("^[a-z0-9-_.]+$") && lex.length > 1 then
            f"`$lex`" else lex

    // We do the escaping here manually to allow for <abbr> tags
    override def toMarkdownEscaped: String = toMarkdown
    override def toMarkdownSimpleEscaped: String = toMarkdownEscaped

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
      val uri = Escaping.escapeHtml(value.getURI) // you never know
      f"${MarkdownUtil.prettyLabelEscaped(label, comment)} ([${ontologies.shortForm(uri)}]($uri))"

    // We do the escaping here manually to allow for <abbr> tags
    override def toMarkdownEscaped: String = toMarkdown
    override def toMarkdownSimpleEscaped: String = toMarkdownEscaped

    override def getTitle: Option[String] = Some(label)

    override def getSortKey: String = label

  object Link:
    def apply(value: model.Resource): Link =
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
            ExternalLink(value)
          else if parts(0).startsWith("#") then
            // Anchor link
            InternalLink(parts(0), parts(1))
          else
            InternalLink(uri, s"${parts(0)} (${parts(1)})")
        case _ if DoiBibliography.isDoiLike(uri) => DoiLink(value)
        case _ => ExternalLink(value)

  trait Link extends DocValue

  case class InternalLink(uri: String, name: String) extends Link:
    def toMarkdown: String = f"[$name]($uri)"
    def getSortKey = name


  case class DoiLink(value: model.Resource) extends Link:
    // We assume here that someone earlier preloaded the needed bibliography
    private val maybeBibEntry = DoiBibliography.getBibliographyFromCache(value.getURI)
    private var annotationId: Option[Int] = None

    override def registerAnnotations(context: DocAnnotationContext): Unit =
      maybeBibEntry.foreach { entry =>
        annotationId = Some(context.registerAnnotation(
          f"BibTeX citation:\n    ``` { .bibtex .rb-wrap-code }\n${entry.bibtex.indent(4).stripLineEnd}\n    ```"
        ))
      }

    def toMarkdown: String =
      if maybeBibEntry.isEmpty then
        println(f"Warning: DOI link without bibliography entry: ${value.getURI}")
      else if annotationId.isEmpty then
        println(f"Warning: DOI link without annotation ID: ${value.getURI}")
      val uri = Escaping.escapeHtml(value.getURI)
      maybeBibEntry.fold
        (f"[$uri]($uri)")
        (entry => urlRegex.replaceAllIn(
          f"${entry.apa.strip} :custom-bibtex:{ .rb-bibtex } (${annotationId.map(_.toString).getOrElse("ERROR")})",
          m => f"[${m.matched}](${m.matched})"
        ))

    // Treat the output from the DOI API as safe.
    // It uses basic tags like <i> for formatting.
    override def toMarkdownEscaped: String = toMarkdown
    override def toMarkdownSimpleEscaped: String = toMarkdownEscaped

    override def getSortKey = value.getURI


  case class ExternalLink(value: model.Resource) extends Link:
    def toMarkdown: String = f"[${value.getURI}](${value.getURI})"
    override def getSortKey = value.getURI

  case class BlankNode(values: Iterable[(DocProp, DocValue)], name: Option[String]) extends DocValue:
    override val isNestedList = true

    override def registerAnnotations(context: DocAnnotationContext): Unit =
      values.foreach { case (_, value) => value.registerAnnotations(context) }

    override def toMarkdown: String = values
      .toSeq
      .filter(!_._1.hidden)
      .sortBy(_._1.weight)
      .map { case (prop, value) =>
        val vMd = if value.isNestedList then
          value.toMarkdownEscaped.linesIterator.map(indent + _).mkString("\n")
        else value.toMarkdownEscaped
        s"\n$indent- **${prop.toMarkdownEscaped}**: $vMd"
      }.mkString

    // We do the escaping here manually because we have nested values
    override def toMarkdownEscaped: String = toMarkdown
    override def toMarkdownSimpleEscaped: String = toMarkdownEscaped

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
      .getOrElse("zzz" + values.map(_._2.getSortKey).toSeq.sorted.hashCode().toString)

  case class List(values: Iterable[DocValue], baseName: Option[String]) extends DocValue:
    override val isNestedList = true

    override def registerAnnotations(context: DocAnnotationContext): Unit =
      values.foreach(_.registerAnnotations(context))

    def toMarkdown: String =
      val baseItemName = baseName.map(Escaping.escapeHtml).getOrElse("Item").strip
      val valuesSorted = values.toSeq.sortBy(_.getSortKey)
      val items = for (value, i) <- valuesSorted.zipWithIndex yield
        if value.isNestedList then
          f"$indent- **${value.getTitle.map(Escaping.escapeHtml).getOrElse(baseItemName)} (${i + 1})**" +
            value.toMarkdownEscaped.linesIterator.map(indent + _).mkString("\n")
        else f"$indent- ${value.toMarkdownEscaped}"
      "\n" + items.mkString("\n")

    // We do the escaping here manually because we have nested values
    override def toMarkdownEscaped: String = toMarkdown

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
      sb.append(allCols.map { col => s"| **${col.toMarkdownEscaped}** " }.mkString("") + "|\n")
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
            .map((_, v) => v.asInstanceOf[DocValue.Literal].value.getInt)
            .headOption
            .getOrElse(1000)
        }
        .foreach { case (row, cols) =>
          sb.append(s"| **$row** ")
          for col <- allCols do
            val value = cols.find((k, _) => k._2 == col).map((_, v) => v)
            value match
              case Some(value) => sb.append(s"| ${value.toMarkdownSimpleEscaped} ")
              case None => sb.append("| _N/A_ ")
          sb.append("|\n")
        }
      sb.toString

    // We do the escaping here manually because we have nested values
    override def toMarkdownEscaped: String = toMarkdown
    override def toMarkdownSimpleEscaped: String = toMarkdownEscaped

    override def getSortKey = ""

trait DocValue:
  val isNestedList = false

  val noIndent = false

  def registerAnnotations(context: DocAnnotationContext): Unit = ()

  protected def toMarkdown: String

  def toMarkdownEscaped: String = Escaping.escapeHtml(toMarkdown)

  protected def toMarkdownSimple: String = toMarkdown

  /**
   * Markdown without nested lists or anything funny – just return a simple MD
   * string that can be used in other formatting (e.g., a list).
   */
  def toMarkdownSimpleEscaped: String = Escaping.escapeHtml(toMarkdownSimple)

  def getTitle: Option[String] = None

  def getSortKey: String
