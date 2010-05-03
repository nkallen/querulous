package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.querulous.test.{FakeQuery, FakeStatsCollector}
import com.twitter.querulous.query.TimingOutStatsCollectingQuery
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


class TimingOutStatsCollectingQuerySpec extends Specification with JMocker {
  "TimingOutStatsCollectingQuery" should {
    Time.freeze()
    val latency = 1.second
    val testQuery = new FakeQuery(List(mock[ResultSet])) {
      override def select[A](f: ResultSet => A) = {
        Time.advance(latency)
        super.select(f)
      }
      override def execute() = {
        Time.advance(latency)
        super.execute()
      }
    }

    "collect stats" in {
      val stats = new FakeStatsCollector

      "selects" >> {
        val query = new TimingOutStatsCollectingQuery(testQuery, "selectTest", stats)
        query.select { _ => 1 } mustEqual List(1)
        stats.times("db-select-timing") mustEqual latency.inMillis
        stats.times("x-db-query-timing-selectTest") mustEqual latency.inMillis
        stats.counts("db-select-count") mustEqual 1
      }

      "executes" >> {
        val query = new TimingOutStatsCollectingQuery(testQuery, "executeTest", stats)
        query.execute
        stats.times("db-execute-timing") mustEqual latency.inMillis
        stats.times("x-db-query-timing-executeTest") mustEqual latency.inMillis
        stats.counts("db-execute-count") mustEqual 1
      }

      "globally" >> {
        val query = new TimingOutStatsCollectingQuery(testQuery, "default", stats)
        query.select { _ => 1 } mustEqual List(1)
        stats.times("db-timing") mustEqual latency.inMillis
      }
    }
  }
}

