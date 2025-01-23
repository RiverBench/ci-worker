package io.github.riverbench.ci_worker
package util.doc

import util.RdfUtil

import scala.collection.mutable

class DocSection(val level: Int, defaultPropGroup: Option[String] = None, isRoot: Boolean = false)
  extends DocAnnotationContext:

  private val entries = mutable.ListBuffer[(DocProp, DocValue)]()
  private val subsections = mutable.ListBuffer[DocSection]()

  private var title = "(empty)"
  private var content = ""
  private var anchor: Option[String] = None
  private var weight: Option[Double] = None

  // Accessors set and get
  def setTitle(title: String): Unit = this.title = title
  def setContent(content: String): Unit = this.content = content
  def setAnchor(anchor: String): Unit = this.anchor = Some(anchor)
  def setWeight(weight: Double): Unit = this.weight = Some(weight)
  def getTitle: String = title

  def addEntry(prop: DocProp, value: DocValue): Unit =
    val group = prop.group.orElse(defaultPropGroup)
    if prop.prop.getURI == RdfUtil.hasDocWeight.getURI then
      value match
        case DocValue.Literal(l) =>
          l.getLexicalForm.toIntOption match
            case Some(i) => setWeight(i)
            case None => ()
        case _ => ()
    else if prop.hidden then ()
    else if isRoot && group.isDefined then
      if !subsections.exists(_.title == group.get) then
        val subSec = addSubsection()
        subSec.setTitle(group.get)
      val sub = subsections.find(_.title == group.get).get
      sub.addEntry(prop.copy(group = None), value)
    else
      value.registerAnnotations(this)
      entries.addOne((prop, value))

  def addSubsection(): DocSection =
    val section = DocSection(level + 1)
    subsections += section
    section

  private def getWeight: Double =
    if weight.isDefined then
      return weight.get

    val selfWeight = if entries.isEmpty then
      0.0
    else
      entries.map(_._1.weight.toDouble).filter(_ < 3_000_000.0).sum / entries.size

    val subWeight = if subsections.isEmpty then 0.0 else
      subsections.map(_.getWeight).filter(_ < 3_000_000.0).sum / subsections.size

    selfWeight + subWeight

  def toMarkdown: String =
    val headerSb = new StringBuilder()
    val anchorMd = anchor.map(a => s" <a name=\"$a\"></a>").getOrElse("")
    headerSb.append(s"${"#"*level}$anchorMd $title\n\n")
    if content.nonEmpty then
      headerSb.append(s"$content\n\n")

    val sb = new StringBuilder()
    for entry <- entries.sortBy(_._1.weight) do
      if entry._2.noIndent then
        sb.append(s"\n${entry._2.toMarkdownEscaped}\n")
      else
        sb.append(s"- **${entry._1.toMarkdownEscaped}**: ${entry._2.toMarkdownEscaped}\n")

    if entries.nonEmpty && subsections.nonEmpty then
      sb.append("\n")

    wrapSection(sb).insert(0, headerSb)

    val sortedSections = subsections
      .sortBy(_.title)
      .sortBy(_.getWeight)

    for sub <- sortedSections do
      sb.append(s"${sub.toMarkdown.strip}\n\n")

    sb.toString
