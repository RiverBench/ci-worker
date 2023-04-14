package io.github.riverbench.ci_worker
package util.doc

import util.{MetadataReader, RdfUtil}

import org.apache.jena.rdf.model.{Literal, Model, Property, RDFNode, Resource}
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.jdk.CollectionConverters.*

object DatasetDocBuilder:
  private val titleProps = Seq(
    RdfUtil.dctermsTitle,
    RDFS.label,
    FOAF.name,
    FOAF.nick,
    RDF.`type`,
  )

  private val nestedSectionProps = Seq(
    RdfUtil.dcatDistribution,
    RdfUtil.hasStatistics,
  )

class DatasetDocBuilder(ontologies: Model, metadata: Model):
  import DatasetDocBuilder.*

  private val groups = new DocGroupRegistry(ontologies)

  def build(): DocSection =
    val rootSection = new DocSection(1)
    val datasetRes = metadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource
    buildSection(datasetRes, rootSection)
    rootSection

  private def buildSection(resource: Resource, section: DocSection): Unit =
    val props = getDocPropsForRes(resource)

    val usedValues = props.flatMap { (prop, nodes) =>
      if nestedSectionProps.exists(p => p.getURI == prop.prop.getURI) then
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
      case node => DocValue.Text(node.toString)
    }
    if values.size > 1 then
      DocValue.List(values, Some(docProp.label))
    else
      values.head

  private def getTitleForProps(props: Iterable[(DocProp, DocValue)]): Option[String] =
    titleProps.flatMap { prop =>
      props.collect {
        case (p, value) if p.prop.getURI == prop.getURI =>
          value.getTitle.getOrElse(value.toMarkdown)
      }
    }.headOption

