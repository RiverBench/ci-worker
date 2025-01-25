package io.github.riverbench.ci_worker
package util.rdf

enum ElementType(val name: String):
  case Graph extends ElementType("graph")
  case Dataset extends ElementType("dataset")
  case Triple extends ElementType("triple")
  case Quad extends ElementType("quad")

enum StreamType(val iriName: String, val docName: String, val isFlat: Boolean, val elementType: ElementType):
  lazy val iri = RdfUtil.pStax + iriName
  lazy val readableName = "[A-Z\\d]".r.replaceAllIn(iriName, m => " " + m.group(0).toLowerCase).trim

  case Graph extends StreamType(
    "graphStream",
    "rdf-graph-stream",
    false,
    ElementType.Graph
  )
  case SubjectGraph extends StreamType(
    "subjectGraphStream",
    "rdf-subject-graph-stream",
    false,
    ElementType.Graph
  )
  case Dataset extends StreamType(
    "datasetStream",
    "rdf-dataset-stream",
    false,
    ElementType.Dataset
  )
  case NamedGraph extends StreamType(
    "namedGraphStream",
    "rdf-named-graph-stream",
    false,
    ElementType.Dataset
  )
  case TimestampedNamedGraph extends StreamType(
    "timestampedNamedGraphStream",
    "timestamped-rdf-named-graph-stream",
    false,
    ElementType.Dataset
  )
  case Triple extends StreamType(
    "flatTripleStream",
    "flat-rdf-triple-stream",
    true,
    ElementType.Triple
  )
  case Quad extends StreamType(
    "flatQuadStream",
    "flat-rdf-quad-stream",
    true,
    ElementType.Quad
  )
