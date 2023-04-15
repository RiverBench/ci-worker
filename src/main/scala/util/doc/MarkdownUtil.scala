package io.github.riverbench.ci_worker
package util.doc

object MarkdownUtil:
  val indent = "    "

  def toPrettyString(label: String, comment: Option[String]): String =
    comment match
      case Some(c) =>
        val cClean = c.replace('"', '\'').replace("\n", " ")
        f"<abbr title=\"$cClean\">$label</abbr>"
      case None => label
