package io.github.riverbench.ci_worker
package util.doc

import util.RdfUtil

import org.apache.jena.rdf.model.{Model, Property, Resource}
import org.apache.jena.vocabulary.RDFS

object DocProp:
  def apply(prop: Resource, ontologies: Model, groups: DocGroupRegistry): DocProp =
    def getString(propType: Property): Option[String] =
      val langString = ontologies.getProperty(prop, propType, "en")
      if langString != null then
        Some(langString.getString)
      else
        val string = ontologies.getProperty(prop, propType)
        if string != null then Some(string.getString) else None

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
      label = getString(RDFS.label).getOrElse(prop.getLocalName),
      comment = getString(RDFS.comment),
      weight = weightVal,
      hidden = hiddenSt != null && hiddenSt.getObject.isLiteral && hiddenSt.getBoolean,
      group = groupVal,
    )

case class DocProp(prop: Resource, label: String, comment: Option[String],
                   weight: Int, hidden: Boolean, group: Option[String]):
  def toMarkdown: String = comment match
    case Some(c) =>
      val cClean = c.replace('"', '\'').replace("\n", " ")
      f"$label<sup>[?](## \"$cClean\")</sup>"
    case None => label

