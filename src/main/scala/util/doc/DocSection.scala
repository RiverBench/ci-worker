package io.github.riverbench.ci_worker
package util.doc

import scala.collection.mutable

class DocSection(val level: Int, val title: String, val content: String):
  val entries = mutable.ListBuffer[(DocProp, DocValue)]()
  val subsections = mutable.ListBuffer[DocSection]()

  def addEntry(prop: DocProp, value: DocValue): Unit =
    if prop.group.isDefined then
      if !subsections.exists(_.title == prop.group.get) then
        addSubsection(prop.group.get, "")
      val sub = subsections.find(_.title == prop.group.get).get
      sub.addEntry(prop.copy(group = None), value)
    else
      entries.addOne((prop, value))

  def addSubsection(title: String, content: String): Unit =
    subsections += DocSection(level + 1, title, content)

  private def getWeight: Double =
    val selfWeight = if entries.isEmpty then 0.0 else
      entries.map(_._1.weight.toDouble).filter(_ < 3_000_000.0).sum / entries.size
    val subWeight = if subsections.isEmpty then 0.0 else
      subsections.map(_.getWeight).sum / subsections.size

    selfWeight + subWeight

  def toMarkdown: String =
    val sb = new StringBuilder()
    sb.append(s"${"#"*level} $title\n\n")
    if content.nonEmpty then
      sb.append(s"$content\n\n")

    for entry <- entries.sortBy(_._1.weight) do
      sb.append(s"- **${entry._1.toMarkdown}**: ${entry._2.toMarkdown}\n")

    if entries.nonEmpty && subsections.nonEmpty then
      sb.append("\n")

    for sub <- subsections.sortBy(_.getWeight) do
      sb.append(s"${sub.toMarkdown}\n\n")

    sb.toString
