package io.github.riverbench.ci_worker
package util.doc

import util.RdfUtil

import org.apache.jena.rdf.model.{Literal, Model, Property, RDFNode, Resource}
import org.apache.jena.vocabulary.RDF

import scala.jdk.CollectionConverters.*

object DocBuilder:
  case class Options(titleProps: Seq[Property], nestedSectionProps: Seq[Property],
                     hidePropsInLevel: Seq[(Int, Property)])

class DocBuilder(ontologies: Model, opt: DocBuilder.Options):
  private val groups = new DocGroupRegistry(ontologies)

  def build(title: String, content: String, rootResource: Resource): DocSection =
    val rootSection = new DocSection(1)
    rootSection.setContent(content)
    buildSection(rootResource, rootSection)
    // Override title
    rootSection.setTitle(title)
    rootSection

  private def buildSection(resource: Resource, section: DocSection): Unit =
    val props = getDocPropsForRes(resource)

    val usedValues = props.flatMap { (prop, nodes) =>
      val isPropHiddenInLevel = opt.hidePropsInLevel.exists { case (l, p) =>
        l == section.level && p.getURI == prop.prop.getURI
      }

      if opt.nestedSectionProps.exists(p => p.getURI == prop.prop.getURI) then
        if !isPropHiddenInLevel then
          val nestedSection = section.addSubsection()
          nestedSection.setWeight(prop.weight)
          nestedSection.setTitle(prop.group.getOrElse(prop.toMarkdown))
          for node <- nodes do
            val itemSection = nestedSection.addSubsection()
            node match
              case res: Resource => buildSection(res, itemSection)
              case _ => println("WARNING: nested section node is not a resource")
        None
      else
        val value = makeValue(prop, nodes)
        if !isPropHiddenInLevel then
          section.addEntry(prop, value)
        Some(prop, value)
    }
    getTitleForProps(usedValues) match
      case Some(title) => section.setTitle(title)
      case None =>

  private def getDocPropsForRes(resource: Resource): Iterable[(DocProp, Seq[RDFNode])] =
    val grouped = resource.listProperties().asScala.toSeq.groupBy(_.getPredicate)
    grouped.flatMap { (prop, statements) =>
      val docProp = DocProp(prop, ontologies, groups)
      if docProp.hidden then
        None
      else
        Some(docProp, statements.map(_.getObject))
    }

  private def getDocValuesForRes(resource: Resource): Iterable[(DocProp, DocValue)] =
    getDocPropsForRes(resource).map { (prop, objects) =>
      prop -> makeValue(prop, objects)
    }

  private def makeValue(docProp: DocProp, objects: Iterable[RDFNode]): DocValue =
    val values = objects.map {
      case lit: Literal => DocValue.Literal(lit)
      case res: Resource if res.isAnon =>
        val nestedValues = getDocValuesForRes(res)
        DocValue.BlankNode(nestedValues, getTitleForProps(nestedValues))
      case res: Resource if docProp.prop.getURI == RDF.`type`.getURI ||
        RdfUtil.isNamedThing(res, Some(ontologies)) =>
        DocValue.RdfNamedThing(res, ontologies)
      case res: Resource => DocValue.Link(res)
      case node => DocValue.Text(node.toString)
    }
    if values.size > 1 then
      DocValue.List(values, Some(docProp.label))
    else
      values.head

  private def getTitleForProps(props: Iterable[(DocProp, DocValue)]): Option[String] =
    opt.titleProps.flatMap { prop =>
      props.collect {
        case (p, value) if p.prop.getURI == prop.getURI =>
          value.getTitle.getOrElse(value.toMarkdown)
      }
    }.headOption
