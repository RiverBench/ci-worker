package io.github.riverbench.ci_worker
package util.doc

import util.Constants

object MarkdownUtil:
  private val sizePrefixes = Seq("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

  val indent = "    "

  def prettyLabel(label: String, comment: Option[String]): String =
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

  def readmeHeader(repoName: String): String =
    f"""<!--
       |--
       |-- THIS FILE IS AUTOGENERATED. DO NOT EDIT.
       |-- Please edit the metadata.ttl file instead. The documentation
       |-- will be regenerated by the CI.
       |--
       |-- You can place additional docs in the /doc directory. Remember to link
       |-- to them from the description in the metadata.ttl file.
       |--
       |-->
       |[![.github/workflows/release.yaml](${Constants.baseRepoUrl}/$repoName/actions/workflows/release.yaml/badge.svg?event=push)](${Constants.baseRepoUrl}/$repoName/actions/workflows/release.yaml)
       |""".stripMargin
