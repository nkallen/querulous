package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.querulous.query.{QueryClass, SqlQueryTimeoutException, StatsCollectingQuery}
import com.twitter.querulous.test.{FakeQuery, FakeStatsCollector}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._


class StatsCollectingQuerySpec extends Specification with JMocker {
  "StatsCollectingQuery" should {
    "collect stats" in {
      Time.withCurrentTimeFrozen { time =>
        val latency = 1.second
        val stats = new FakeStatsCollector
        val testQuery = new FakeQuery(List(mock[ResultSet])) {
          override def select[A](f: ResultSet => A) = {
            time.advance(latency)
            super.select(f)
          }
        }
        val statsCollectingQuery = new StatsCollectingQuery(testQuery, QueryClass.Select, stats)

        statsCollectingQuery.select { _ => 1 } mustEqual List(1)

        stats.counts("db-select-count") mustEqual 1
        stats.times("db-timing") mustEqual latency.inMillis
      }
    }

    "collect timeout stats" in {
      Time.withCurrentTimeFrozen { time =>
        val stats = new FakeStatsCollector
        val testQuery = new FakeQuery(List(mock[ResultSet]))
        val statsCollectingQuery = new StatsCollectingQuery(testQuery, QueryClass.Select, stats)
        val e = new SqlQueryTimeoutException(0.seconds)

        statsCollectingQuery.select { _ => throw e } must throwA[SqlQueryTimeoutException]

        stats.counts("db-query-timeout-count") mustEqual 1
        stats.counts("db-query-" + QueryClass.Select.name + "-timeout-count") mustEqual 1
      }
    }
  }
}

