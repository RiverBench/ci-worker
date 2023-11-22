package io.github.riverbench.ci_worker
package util

import org.apache.pekko.stream.SinkShape
import org.apache.pekko.stream.scaladsl.*

object StreamUtil:

  /**
   * Creates a sink that broadcasts the incoming elements to all the given sinks.
   * The materialized value of the returned sink is a sequence of the materialized values of the given sinks.
   * @param sinks the sinks to broadcast to
   * @tparam T the type of the elements
   * @tparam Mat the type of the materialized values of the sinks
   * @return the sink
   */
  def broadcastSink[T, Mat](sinks: Seq[Sink[T, Mat]]): Sink[T, Seq[Mat]] =
    Sink.fromGraph(GraphDSL.create(sinks) { implicit builder =>
      sinkList =>
        import GraphDSL.Implicits.*
        val inFlow = builder.add(Flow[T])
        val inBroad = builder.add(Broadcast[T](sinkList.size))
        inFlow.out ~> inBroad
        for sink <- sinkList do
          inBroad ~> sink

        SinkShape(inFlow.in)
    })
