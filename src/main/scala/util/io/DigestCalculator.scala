/*
 * Copyright 2016 Dennis Vriend
 *  source: https://github.com/dnvriend/akka-stream-extensions/blob/87d0afe9a0e7024423f37208bb875b8260e3008b/src/main/scala/akka/stream/scaladsl/extension/io/DigestCalculator.scala
 * Copyright 2022 Piotr Sowiński
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.riverbench.ci_worker
package util.io

import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.stage.*
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.Future
import scala.util.{Success, Try}

final case class DigestResult(messageDigest: ByteString, status: Try[Done])

sealed trait DigAlgorithm
object DigAlgorithm {
  case object MD2 extends DigAlgorithm
  case object MD5 extends DigAlgorithm
  case object `SHA-1` extends DigAlgorithm
  case object `SHA-256` extends DigAlgorithm
  case object `SHA-384` extends DigAlgorithm
  case object `SHA-512` extends DigAlgorithm
}

/**
 * The DigestCalculator transforms/digests a stream of org.apache.pekko.util.ByteString to a
 * DigestResult according to a given Algorithm
 */
object DigestCalculator {

  def apply(algorithm: DigAlgorithm): Flow[ByteString, DigestResult, NotUsed] =
    Flow.fromGraph[ByteString, DigestResult, NotUsed](new DigestCalculator(algorithm))

  def flow(algorithm: DigAlgorithm): Flow[ByteString, DigestResult, NotUsed] =
    apply(algorithm)

  /**
   * Returns the String encoded as Hex representation of the digested stream of [[org.apache.pekko.util.ByteString]]
   */
  def hexString(algorithm: DigAlgorithm): Flow[ByteString, String, NotUsed] =
    flow(algorithm).map(res => res.messageDigest.toArray.map("%02x".format(_)).mkString).fold("")(_ + _)

  def sink(algorithm: DigAlgorithm): Sink[ByteString, Future[DigestResult]] =
    flow(algorithm).toMat(Sink.head)(Keep.right)

  def source(algorithm: DigAlgorithm, text: String): Source[String, NotUsed] =
    Source.single(ByteString(text)).via(hexString(algorithm))

  /**
   * Internal class performing the actual calculation
   * @param algorithm algorithm to use
   */
  private class DigestCalculator(algorithm: DigAlgorithm) extends GraphStage[FlowShape[ByteString, DigestResult]]:
    val in: Inlet[ByteString] = Inlet("DigestCalculator.in")
    val out: Outlet[DigestResult] = Outlet("DigestCalculator.out")
    override val shape: FlowShape[ByteString, DigestResult] = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      val digest = java.security.MessageDigest.getInstance(algorithm.toString)

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val chunk = grab(in)
          digest.update(chunk.toArray)
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          emit(out, DigestResult(ByteString(digest.digest()), Success(Done)))
          completeStage()
        }
      })
    }
}



