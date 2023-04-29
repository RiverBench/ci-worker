package io.github.riverbench.ci_worker
package util.doc

import scala.collection.mutable

class DocSection(val level: Int):
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
    if level == 1 && prop.group.isDefined then
      if !subsections.exists(_.title == prop.group.get) then
        val subSec = addSubsection()
        subSec.setTitle(prop.group.get)
      val sub = subsections.find(_.title == prop.group.get).get
      sub.addEntry(prop.copy(group = None), value)
    else
      entries.addOne((prop, value))

  def addSubsection(): DocSection =
    val section = DocSection(level + 1)
    subsections += section
    section

  private def getWeight: Double =
    val selfWeight = if weight.isDefined then
      weight.get
    else if entries.isEmpty then
      0.0
    else
      entries.map(_._1.weight.toDouble).filter(_ < 3_000_000.0).sum / entries.size

    val subWeight = if subsections.isEmpty then 0.0 else
      subsections.map(_.getWeight).filter(_ < 3_000_000.0).sum / subsections.size

    selfWeight + subWeight

  def toMarkdown: String =
    val sb = new StringBuilder()
    val anchorMd = anchor.map(a => s" <a name=\"$a\"></a>").getOrElse("")
    sb.append(s"${"#"*level}$anchorMd $title\n\n")
    if content.nonEmpty then
      sb.append(s"$content\n\n")

    for entry <- entries.sortBy(_._1.weight) do
      sb.append(s"- **${entry._1.toMarkdown}**: ${entry._2.toMarkdown}\n")

    if entries.nonEmpty && subsections.nonEmpty then
      sb.append("\n")

    val sortedSections = if level == 1 then
      subsections.sortBy(_.getWeight)
    else
      subsections.sortBy(_.title)

    for sub <- sortedSections do
      sb.append(s"${sub.toMarkdown.strip}\n\n")

    sb.toString
