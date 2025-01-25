package io.github.riverbench.ci_worker
package util.rdf

import org.apache.jena.graph.Node
import org.apache.jena.rdf.model.Resource
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.jena.vocabulary.RDF

import scala.jdk.CollectionConverters.*


sealed trait SubjectNodeShape:
  def findInDs(ds: DatasetGraph): Seq[Node]

object SubjectNodeShape:
  case class TargetClass(classIri: Resource) extends SubjectNodeShape:
    override def findInDs(ds: DatasetGraph) =
      ds.find(null, null, RDF.`type`.asNode(), classIri.asNode()).asScala
        .map(_.getSubject)
        .toSeq

  case class TargetSubjectOf(propertyIri: Resource) extends SubjectNodeShape:
    override def findInDs(ds: DatasetGraph) =
      ds.find(null, null, propertyIri.asNode(), null).asScala
        .map(_.getSubject)
        .toSeq

  case class TargetObjectOf(propertyIri: Resource) extends SubjectNodeShape:
    override def findInDs(ds: DatasetGraph) =
      ds.find(null, null, propertyIri.asNode(), null).asScala
        .map(_.getObject)
        .toSeq

  case class TargetCustom(customInstance: Resource) extends SubjectNodeShape:
    override def findInDs(ds: DatasetGraph) = customInstance match
      case RdfUtil.`yagoTarget` => ds.find(null, null, null, null).asScala
        .map(_.getSubject)
        .filter(_.isNodeTriple)
        .map(_.getTriple.getSubject)
        .toSeq
      case _ => throw new Exception(s"Unknown custom target instance: $customInstance")

  def fromElementSplit(elementSplit: Resource): Seq[SubjectNodeShape] =
    elementSplit.listProperties(RdfUtil.hasSubjectShape)
      .asScala
      .flatMap(shSt => {
        shSt.getResource.listProperties().asScala
          .flatMap(st => {
            st.getPredicate match
              case RdfUtil.shTargetClass => Some(TargetClass(st.getObject.asResource()))
              case RdfUtil.shTargetSubjectsOf => Some(TargetSubjectOf(st.getObject.asResource()))
              case RdfUtil.shTargetObjectsOf => Some(TargetObjectOf(st.getObject.asResource()))
              case RdfUtil.targetCustom => Some(TargetCustom(st.getObject.asResource()))
              case _ => None
          })
          .toSeq.headOption
      })
      .toSeq
