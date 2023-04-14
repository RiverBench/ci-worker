package io.github.riverbench.ci_worker
package util.doc

import util.{MetadataReader, RdfUtil}

import org.apache.jena.rdf.model.{Literal, Model, Property, Resource}
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.jdk.CollectionConverters.*

object DatasetDocBuilder:
  private val titleProps = Seq(
    RdfUtil.dctermsTitle,
    RDFS.label,
    RDF.`type`,
  )

class DatasetDocBuilder(ontologies: Model, metadata: Model):

  private val groups = new DocGroupRegistry(ontologies)

  def build(): DocSection =
    val mi = MetadataReader.fromModel(metadata)
    val rootSection = new DocSection(1, f"Dataset ${mi.identifier}", "")

    val datasetRes = metadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource

    // TODO: nesting sections

    buildSection(datasetRes, rootSection)

    rootSection

  private def buildSection(resource: Resource, section: DocSection): Unit =
    val props = getPropsForResource(resource)
    for (prop, value) <- props do
      section.addEntry(prop, value)

  private def getPropsForResource(resource: Resource): Iterable[(DocProp, DocValue)] =
    val grouped = resource.listProperties().asScala.toSeq.groupBy(_.getPredicate)
    grouped.flatMap { (prop, statements) =>
      val docProp = DocProp(prop, ontologies, groups)
      if docProp.hidden then
        None
      else
        val values = statements.map(st => {
          st.getObject match
            case lit: Literal => DocValue.Literal(lit)
            case res: Resource if res.isAnon =>
              val nestedProps = getPropsForResource(res)
              DocValue.BlankNode(nestedProps, getTitleForProps(nestedProps))
            case res: Resource if docProp.prop.getURI == RDF.`type`.getURI ||
              RdfUtil.isNamedThing(res, Some(ontologies)) =>
                DocValue.RdfNamedThing(res, ontologies)
            case _ => DocValue.Text(st.getObject.toString)
        })
        if values.size > 1 then
          Some(docProp, DocValue.List(values, Some(docProp.label)))
        else
          Some(docProp, values.head)
    }

  private def getTitleForProps(props: Iterable[(DocProp, DocValue)]): Option[String] =
    DatasetDocBuilder.titleProps.flatMap { prop =>
      props.collect {
        case (p, value) if p.prop.getURI == prop.getURI =>
          value.getTitle.getOrElse(value.toMarkdown)
      }
    }.headOption

