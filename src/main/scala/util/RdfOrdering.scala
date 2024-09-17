package io.github.riverbench.ci_worker
package util

import org.apache.jena.sparql.core.Quad

object RdfOrdering:

  given Ordering[Quad] with
    def compare(x: Quad, y: Quad): Int =
      val cmpGraph = x.getGraph.toString.compareTo(y.getGraph.toString)
      if cmpGraph != 0 then cmpGraph
      else
        val cmpSubject = x.getSubject.toString.compareTo(y.getSubject.toString)
        if cmpSubject != 0 then cmpSubject
        else
          val cmpPredicate = x.getPredicate.toString.compareTo(y.getPredicate.toString)
          if cmpPredicate != 0 then cmpPredicate
          else
            x.getObject.toString.compareTo(y.getObject.toString)
