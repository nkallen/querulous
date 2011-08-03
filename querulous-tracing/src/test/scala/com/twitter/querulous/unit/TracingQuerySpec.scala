package com.twitter.querulous.unit

import java.sql.Connection
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.finagle.tracing.{Record, TraceId, Tracer}
import com.twitter.querulous.query._

class TracingQuerySpec extends Specification with JMocker {
  "TracingQuery" should {

    "add records as query is executed" in {
      val queryString = "select * from users"
      val tracer = mock[Tracer]
      val connection = mock[Connection]

      expect {
        one(tracer).sampleTrace(a[TraceId])
        one(connection).getClientInfo("ClientHostname")
        one(connection).prepareStatement(queryString)
        exactly(5).of(tracer).record(a[Record])
      }

      val query = new SqlQuery(connection, queryString)
      val tracingQuery = new TracingQuery(query, connection, QueryClass.Select, "service", tracer)
      tracingQuery.execute()
    }
  }
}
