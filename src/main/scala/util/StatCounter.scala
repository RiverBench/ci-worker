package io.github.riverbench.ci_worker
package util

import org.apache.datasketches.hll.HllSketch
import org.apache.jena.datatypes.xsd.XSDDatatype.*
import org.apache.jena.rdf.model.Resource

//noinspection UnstableApiUsage
object StatCounter:
  case class Result[TStat <: Double | Long]
  (
    sum: Option[TStat], mean: Double, stDev: Double, min: TStat, max: TStat, uniqueCount: Option[Long],
    uniqueLowerBound: Option[Long], uniqueUpperBound: Option[Long]
  ):
    def addToRdf(statRes: Resource): Unit =
      sum.foreach(c => statRes.addLiteral(RdfUtil.sum, c))
      statRes.addLiteral(RdfUtil.mean, mean)
      statRes.addLiteral(RdfUtil.stDev, stDev)
      statRes.addLiteral(RdfUtil.minimum, min)
      statRes.addLiteral(RdfUtil.maximum, max)
      uniqueCount.foreach(c => statRes.addLiteral(RdfUtil.uniqueCount, c))
      uniqueLowerBound.foreach(c => statRes.addLiteral(RdfUtil.uniqueCountLowerBound, c))
      uniqueUpperBound.foreach(c => statRes.addLiteral(RdfUtil.uniqueCountUpperBound, c))


trait StatCounter[T, TStat <: Double | Long]:
  def add(values: Seq[T]): Unit
  def addUnique(values: Iterable[T]): Unit
  def result: StatCounter.Result[TStat]


class UncountableStatCounter extends StatCounter[Double, Double]:
  import StatCounter.*

  private var count: Long = 0
  private var sum: BigDecimal = 0
  private var sumSq: BigDecimal = 0
  private var min: Double = Double.MaxValue
  private var max: Double = Double.MinValue

  override def add(values: Seq[Double]): Unit = addUnique(values)

  override def addUnique(values: Iterable[Double]): Unit =
    values.foreach(addOne)

  def addOne(value: Double): Unit = this.synchronized {
    count += 1
    sum += value
    sumSq += value * value
    if value < min then min = value
    if value > max then max = value
  }

  override def result: Result[Double] = this.synchronized {
    val mean = sum.toDouble / count
    val stDev = Math.sqrt(sumSq.toDouble / count - mean * mean)
    Result(None, mean, stDev, min, max, None, None, None)
  }


class LightStatCounter[T] extends StatCounter[T, Long]:
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

  def result: Result[Long] = this.synchronized {
    val mean = sum.toDouble / count
    val stDev = Math.sqrt(sumSq.toDouble / count - mean * mean)
    Result(Some(sum), mean, stDev, min, max, None, None, None)
  }


class SketchStatCounter extends LightStatCounter[String]:
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

  override def result: Result[Long] = sketch.synchronized {
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

  override def result: StatCounter.Result[Long] =
    super.result.copy(uniqueCount = Some(set.size.toLong))
