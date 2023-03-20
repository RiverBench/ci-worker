package io.github.riverbench.ci_worker
package util

import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}

import java.util

//noinspection UnstableApiUsage
object StatCounter:
  case class Result(count: Long, sum: Long, mean: Double, stDev: Double, min: Long, max: Long,
                    uniqueCount: Option[Long])

  implicit val stringFunnel: Funnel[String] =
    (from: String, into: PrimitiveSink) => into.putBytes(from.getBytes)

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
    Result(count, sum, mean, stDev, min, max, None)
  }

//noinspection UnstableApiUsage
class StatCounter[T : Funnel](size: Long) extends LightStatCounter[T]:
  import StatCounter.*

  private val bloomFilter = BloomFilter.create[T](implicitly[Funnel[T]], size, 0.01)

  override def add(values: Seq[T]): Unit =
    // the bloom filter is thread-safe
    values.foreach(bloomFilter.put)

    // but the counter is not
    lightAdd(values.distinct.size)

  override def addUnique(values: Iterable[T]): Unit =
    values.foreach(bloomFilter.put)
    lightAdd(values.size)

  override def result: Result =
    super.result.copy(uniqueCount = Some(bloomFilter.approximateElementCount))
