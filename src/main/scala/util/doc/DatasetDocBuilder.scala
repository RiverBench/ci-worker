package io.github.riverbench.ci_worker
package util.doc

import io.github.riverbench.ci_worker.util.{MetadataReader, RdfUtil}
import org.apache.jena.rdf.model.{Model, Property, Resource}
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.jdk.CollectionConverters.*

class DatasetDocBuilder(ontologies: Model, metadata: Model):

  def build(): DocSection =
    val mi = MetadataReader.fromModel(metadata)
    val groups = new DocGroupRegistry(ontologies)
    val rootSection = new DocSection(1, f"Dataset ${mi.identifier}", "")

    val datasetRes = metadata.listSubjectsWithProperty(RDF.`type`, RdfUtil.Dataset).next.asResource

    // TODO: type handling
    // TODO: nesting sections
    // TODO: bNodes
    // TODO: label capitalization

    val grouped = datasetRes.listProperties().asScala.toSeq.groupBy(_.getPredicate)
    for (prop, statements) <- grouped do
      val docProp = DocProp(prop, ontologies, groups)
      val values = statements.map(st => DocValue.apply(st.getObject))

      if values.size > 1 then
        rootSection.addEntry(docProp, DocValue.List(values))
      else
        rootSection.addEntry(docProp, values.head)

    rootSection

