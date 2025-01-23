package io.github.riverbench.ci_worker
package util.doc

trait DocAnnotationContext:
  def annotationsEnabled: Boolean

  private val annotations = collection.mutable.ArrayBuffer.empty[String]

  def registerAnnotation(text: String): Int =
    annotations.addOne(text)
    annotations.size

  protected def wrapSection(text: StringBuilder): StringBuilder =
    if !annotationsEnabled || annotations.isEmpty then
      text
    else
      text
        .insert(0, "<div class=\"annotate\" markdown>\n\n")
        .append("\n</div>\n\n")
      for i <- annotations.indices do
        text.append(f"${i + 1}. ${annotations(i)}\n")
      text
