package io.github.riverbench.ci_worker
package util.doc

object MarkdownUtil:
  def toPrettyString(label: String, comment: Option[String]): String =
    comment match
      case Some(c) =>
        val cClean = c.replace('"', '\'').replace("\n", " ")
        f"$label<sup>[?](## \"$cClean\")</sup>"
      case None => label
