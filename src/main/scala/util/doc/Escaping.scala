package io.github.riverbench.ci_worker
package util.doc

import scala.collection.mutable

object Escaping:
  private val protectedStrings: mutable.Set[String] = mutable.Set.empty

  /**
   * Protect the string from being escaped again in the future.
   *
   * Should only be used occasionally for strings that we escape before we insert them into the metadata RDF graph.
   * @param v escaped string
   */
  def protectString(v: String): Unit = protectedStrings.add(v)

  /**
   * Escapes HTML tags in Markdown content that should not contain HTML.
   */
  def escapeHtml(v: String): String =
    if protectedStrings.contains(v) then v
    else v.replace("<", "&lt;").replace(">", "&gt;")
