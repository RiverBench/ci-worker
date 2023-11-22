package io.github.riverbench.ci_worker
package util.doc

import util.RdfUtil

import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.vocabulary.RDFS

object DocProp:
  def apply(prop: Resource, ontologies: Model, groups: DocGroupRegistry): DocProp =
    val weightSt = ontologies.getProperty(prop, RdfUtil.hasDocWeight)
    val weightVal = if weightSt == null || !weightSt.getObject.isLiteral then
      Int.MaxValue
    else
      weightSt.getInt

    val hiddenSt = ontologies.getProperty(prop, RdfUtil.isHiddenInDoc)
    val groupSt = ontologies.getProperty(prop, RdfUtil.hasDocGroup)
    val groupVal = if groupSt == null || !groupSt.getObject.isURIResource then
      None
    else
      groups.getDocGroup(groupSt.getObject.toString)

    DocProp(
      prop,
      label = RdfUtil.getPrettyLabel(prop, Some(ontologies)),
      comment = RdfUtil.getString(prop, RDFS.comment, Some(ontologies)),
      weight = weightVal,
      hidden = hiddenSt != null && hiddenSt.getObject.isLiteral && hiddenSt.getBoolean,
      group = groupVal,
    )

case class DocProp(prop: Resource, label: String, comment: Option[String],
                   weight: Int, hidden: Boolean, group: Option[String]):
  def toMarkdown: String = MarkdownUtil.prettyLabel(label, comment)
