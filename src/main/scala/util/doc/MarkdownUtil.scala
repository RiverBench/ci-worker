package io.github.riverbench.ci_worker
package util.doc

object MarkdownUtil:
  private val sizePrefixes = Seq("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

  val indent = "    "

  def toPrettyString(label: String, comment: Option[String]): String =
    comment match
      case Some(c) =>
        val cClean = c.replace('"', '\'').replace("\n", " ")
        f"<abbr title=\"$cClean\">$label</abbr>"
      case None => label

  def formatInt(v: String): String =
    v.strip
      .reverse
      .grouped(3)
      .mkString(",")
      .reverse

  def formatDouble(v: String): String =
    v.strip.toDoubleOption
      .map(d => f"$d%,.2f")
      .getOrElse(v)

  def formatSize(v: Long): String =
    def inner(d: Double): (Int, Double) =
      if d < 1024 then
        (0, d)
      else
        val (i, d2) = inner(d / 1024d)
        (i + 1, d2)

    val (level, d) = inner(v.toDouble)
    f"$d%.2f ${sizePrefixes(level)}"

  def formatMetadataLinks(baseUrl: String, suffix: String = ""): String =
    f"**[Turtle]($baseUrl.ttl$suffix)**, **[N-Triples]($baseUrl.nt$suffix)**, " +
      f"**[RDF/XML]($baseUrl.rdf$suffix)**, **[Jelly]($baseUrl.jelly$suffix)**"
