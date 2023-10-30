package io.github.riverbench.ci_worker
package util


enum ElementType(val name: String):
  case Graph extends ElementType("graph")
  case Dataset extends ElementType("dataset")
  case Triple extends ElementType("triple")
  case Quad extends ElementType("quad")

enum StreamType(val iriName: String, val isFlat: Boolean, val elementType: ElementType):
  lazy val iri = RdfUtil.pStax + iriName
  lazy val readableName = "[A-Z\\d]".r.replaceAllIn(iriName, m => " " + m.group(0).toLowerCase).trim

  case Graph extends StreamType("graphStream", false, ElementType.Graph)
  case SubjectGraph extends StreamType("subjectGraphStream", false, ElementType.Graph)
  case Dataset extends StreamType("datasetStream", false, ElementType.Dataset)
  case NamedGraph extends StreamType("namedGraphStream", false, ElementType.Dataset)
  case TimestampedNamedGraph extends StreamType("timestampedNamedGraphStream", false, ElementType.Dataset)
  case Triple extends StreamType("flatTripleStream", true, ElementType.Triple)
  case Quad extends StreamType("flatQuadStream", true, ElementType.Quad)
