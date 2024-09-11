package io.github.riverbench.ci_worker
package util

import org.apache.datasketches.hll.HllSketch
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.Resource

//noinspection UnstableApiUsage
object StatCounter:
  case class Result(sum: Long, mean: Double, stDev: Double, min: Long, max: Long,
                    uniqueCount: Option[Long], uniqueLowerBound: Option[Long], uniqueUpperBound: Option[Long]):
    def addToRdf(statRes: Resource): Unit =
      statRes.addProperty(RdfUtil.sum, sum.toString, XSDinteger)
      statRes.addProperty(RdfUtil.mean, mean.toString, XSDdecimal)
      statRes.addProperty(RdfUtil.stDev, stDev.toString, XSDdecimal)
      statRes.addProperty(RdfUtil.minimum, min.toString, XSDinteger)
      statRes.addProperty(RdfUtil.maximum, max.toString, XSDinteger)
      uniqueCount.foreach(c => statRes.addProperty(RdfUtil.uniqueCount, c.toString, XSDinteger))
      uniqueLowerBound.foreach(c => statRes.addProperty(RdfUtil.uniqueCountLowerBound, c.toString, XSDinteger))
      uniqueUpperBound.foreach(c => statRes.addProperty(RdfUtil.uniqueCountUpperBound, c.toString, XSDinteger))

class LightStatCounter[T]:
  import StatCounter.*

  private var count: Long = 0
  private var sum: Long = 0
  private var sumSq: Long = 0 // This may overflow for absurdly large datasets (~10B elements)
  private var min: Long = Long.MaxValue
  private var max: Long = Long.MinValue

  def add(values: Seq[T]): Unit =
    lightAdd(values.distinct.size)

  def addUnique(values: Iterable[T]): Unit =
    lightAdd(values.size)

  def lightAdd(c: Long): Unit = this.synchronized {
    count += 1
    sum += c
    sumSq += c * c
    if c < min then min = c
    if c > max then max = c
  }

  def result: Result = this.synchronized {
    val mean = sum.toDouble / count
    val stDev = Math.sqrt(sumSq.toDouble / count - mean * mean)
    Result(sum, mean, stDev, min, max, None, None, None)
  }

class StatCounter extends LightStatCounter[String]:
  import StatCounter.*

  private val sketch = HllSketch(16)

  override def add(values: Seq[String]): Unit =
    sketch.synchronized {
      values.foreach(sketch.update)
    }
    lightAdd(values.distinct.size)

  override def addUnique(values: Iterable[String]): Unit =
    sketch.synchronized {
      values.foreach(sketch.update)
    }
    lightAdd(values.size)

  override def result: Result = sketch.synchronized {
    super.result.copy(
      uniqueCount = Some(sketch.getEstimate.toLong),
      uniqueLowerBound = Some(sketch.getLowerBound(2).toLong),
      uniqueUpperBound = Some(sketch.getUpperBound(2).toLong)
    )
  }

// uses sets instead of bloom filters
class PreciseStatCounter[T] extends LightStatCounter[T]:
  private val set: scala.collection.mutable.HashSet[T] = scala.collection.mutable.HashSet.empty

  override def add(values: Seq[T]): Unit =
    set ++= values
    lightAdd(values.distinct.size)

  override def addUnique(values: Iterable[T]): Unit =
    set ++= values
    lightAdd(values.size)

  override def result: StatCounter.Result =
    super.result.copy(uniqueCount = Some(set.size.toLong))
