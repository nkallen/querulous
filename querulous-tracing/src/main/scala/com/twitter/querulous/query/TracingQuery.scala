package com.twitter.querulous.query

import com.twitter.finagle.tracing.{Tracer, Annotation, Trace}
import java.nio.ByteBuffer
import java.sql.Connection
import java.net.{UnknownHostException, InetSocketAddress, InetAddress}

/**
 * Adds trace annotations to capture data about the query.
 * This data is then processed and sent off with a Finagle compatible tracer.
 */
class TracingQuery(query: Query, connection: Connection, queryClass: QueryClass,
                   serviceName: String, tracer: Tracer) extends QueryProxy(query: Query) {

  override protected def delegate[A](f: => A) = {
    Trace.unwind {
      Trace.pushTracer(tracer)
      // generate the id for this span, decide to sample or not
      val nextId = Trace.nextId
      if (Trace.id.sampled || tracer.sampleTrace(nextId)) {
        Trace.pushId(nextId.copy(sampled = true))
      } else {
        Trace.pushId(nextId)
      }

      try {
        // don't know the port
        Trace.recordClientAddr(new InetSocketAddress(
          InetAddress.getByName(connection.getClientInfo("ClientHostname")), 0))
      } catch {
        case e: UnknownHostException => () // just don't set it if we can't find it
      }

      // we want to know which query caused these timings
      getRootQuery match {
        case q: SqlQuery =>
          Trace.record(Annotation.BinaryAnnotation("querulous.query",
            ByteBuffer.wrap(q.query.getBytes())))
          Trace.recordRpcname(serviceName, queryClass.name)
        case _ => ()
      }

      // send request and time it
      Trace.record(Annotation.ClientSend())
      val rv = f
      Trace.record(Annotation.ClientRecv())
      rv
    }
  }
}

class TracingQueryFactory(queryFactory: QueryFactory, serviceName: String, tracer: Tracer)
  extends QueryFactory {

  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    new TracingQuery(queryFactory(connection, queryClass, query, params: _*),
      connection, queryClass, serviceName, tracer)
  }
}
