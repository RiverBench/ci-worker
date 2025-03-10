package io.github.riverbench.ci_worker
package util.doc

import io.github.riverbench.ci_worker.util.rdf.RdfUtil
import org.apache.jena.rdf.model.{Container, Literal, Model, Property, RDFNode, Resource}
import org.apache.jena.sparql.util.graph.{GNode, GraphList}
import org.apache.jena.vocabulary.RDF

import scala.jdk.CollectionConverters.*

object DocBuilder:
  case class Options(
    titleProps: Seq[Property] = Seq(),
    nestedSectionProps: Seq[(Int, Property)] = Seq(),
    hidePropsInLevel: Seq[(Int, Property)] = Seq(),
    defaultPropGroup: Option[String] = None,
    tabularProps: Seq[Property] = Seq(),
    startHeadingLevel: Int = 1,
    // Arguments to the partial function: predicate, object.
    customValueFormatters: PartialFunction[(Resource, RDFNode), DocValue] = PartialFunction.empty,
    customSectionContentGen: Map[Resource, Seq[RDFNode] => String] = Map(),
    // Whether to enable annotation syntax -- disable this for .md files for GitHub
    // This disables the BibTeX annotations
    enableAnnotations: Boolean = true,
  )

class DocBuilder(ontologies: Model, opt: DocBuilder.Options):
  private val groups = new DocGroupRegistry(ontologies)

  def build(title: String, content: String, rootResource: Resource): DocSection =
    val rootSection = new DocSection(
      opt.startHeadingLevel, opt.defaultPropGroup, true,
      annotationsEnabled = opt.enableAnnotations,
    )
    rootSection.setContent(content)
    buildSection(rootResource, rootSection)
    // Override title
    rootSection.setTitle(title)
    rootSection

  def buildSection(resource: Resource, section: DocSection): Unit =
    val props = getDocPropsForRes(resource)

    val usedValues = props.flatMap { (prop, nodes) =>
      val isPropHiddenInLevel = opt.hidePropsInLevel.exists { case (l, p) =>
        l == section.level && p.getURI == prop.prop.getURI
      }
      val isPropNestedSection = opt.nestedSectionProps.exists { case (l, p) =>
        l == section.level && p.getURI == prop.prop.getURI
      }
      val isPropTabular = opt.tabularProps.contains(prop.prop)

      if isPropNestedSection then
        if !isPropHiddenInLevel then
          val nestedSection = section.addSubsection()
          nestedSection.setWeight(prop.weight)
          nestedSection.setTitle(prop.group.getOrElse(prop.toMarkdownEscaped))
          val customContent = opt.customSectionContentGen.get(prop.prop).map(_(nodes))
          nestedSection.setContent(customContent.getOrElse(""))
          for node <- nodes do
            val itemSection = nestedSection.addSubsection()
            node match
              case res: Resource =>
                if res.isURIResource && res.getURI.contains('#') then
                  itemSection.setAnchor(res.getURI.split('#').last)
                buildSection(res, itemSection)
              case _ => println("WARNING: nested section node is not a resource")
        None
      else if isPropTabular then
        val value = makeTable(nodes)
        if !isPropHiddenInLevel then
          section.addEntry(prop, value)
        Some(prop, value)
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
      Some(docProp, statements.map(_.getObject))
    }

  private def getDocValuesForRes(resource: Resource): Iterable[(DocProp, DocValue)] =
    getDocPropsForRes(resource).map { (prop, objects) =>
      prop -> makeValue(prop, objects)
    }

  private def makeTable(objects: Iterable[RDFNode]): DocValue =
    val tableMap = objects.flatMap {
      case rowRes: Resource =>
        val rowValues = getDocValuesForRes(rowRes)
        val rowTitle = rowValues.find(_._1.prop == RDF.`type`)
          .map(_._2.toMarkdownSimpleEscaped.replaceAll("\\s*\\(\\[.*?\\)$", ""))
          .getOrElse(getTitleForProps(rowValues).getOrElse(Escaping.escapeHtml(rowRes.toString)))
        Some(rowValues.map((colProp, v) => {
          (rowTitle, colProp) -> v
        }))
      case _ =>
        println("WARNING: table row node is not a resource")
        None
    }.flatten.toMap
    DocValue.Table(tableMap)

  private def makeValue(docProp: DocProp, objects: Iterable[RDFNode]): DocValue =
    def makeValueInternal(o: RDFNode): DocValue =
      (docProp.prop, o) match
        case opt.customValueFormatters(value) => value
        case (RdfUtil.dcatByteSize, lit: Literal) =>
          DocValue.SizeLiteral(lit.getLexicalForm)
        case (_, lit: Literal) => DocValue.Literal(lit)
        case (_, res: Resource) if res.isAnon =>
          val nestedValues = getDocValuesForRes(res)
          DocValue.BlankNode(nestedValues, getTitleForProps(nestedValues))
        case (prop, res: Resource) if prop == RDF.`type` ||
          RdfUtil.isNamedThing(res, Some(ontologies)) =>
          DocValue.RdfNamedThing(res, ontologies)
        case (_, res: Resource) => DocValue.Link(res)
        case (_, node) => DocValue.Text(node.toString)

    val values = objects.map { o =>
      val list = GraphList.members(GNode.create(o.getModel.getGraph, o.asNode)).asScala
        .map(n => o.getModel.getRDFNode(n))
      o match
        case _ if list.nonEmpty =>
          val nestedValues = list.map(makeValueInternal)
          DocValue.List(nestedValues, Some(docProp.label))
        case container: Container =>
          val nestedValues = container.iterator.asScala.toSeq.map(makeValueInternal)
          DocValue.List(nestedValues, Some(docProp.label))
        case node => makeValueInternal(node)
    }
    if values.size > 1 then
      DocValue.List(values, Some(docProp.label))
    else
      values.head

  private def getTitleForProps(props: Iterable[(DocProp, DocValue)]): Option[String] =
    opt.titleProps.flatMap { prop =>
      props.collect {
        case (p, value) if p.prop.getURI == prop.getURI =>
          value.getTitle.map(Escaping.escapeHtml).getOrElse(value.toMarkdownSimpleEscaped)
      }
    }.headOption
