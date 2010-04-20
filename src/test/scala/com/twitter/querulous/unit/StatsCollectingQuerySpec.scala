package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.querulous.query.StatsCollectingQuery
import com.twitter.querulous.test.{FakeQuery, FakeStatsCollector}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


class StatsCollectingQuerySpec extends Specification with JMocker {
  "StatsCollectingQuery" should {
    Time.freeze()
    val latency = 1.second
    val testQuery = new FakeQuery(List(mock[ResultSet])) {
      override def select[A](f: ResultSet => A) = {
        Time.advance(latency)
        super.select(f)
      }
    }

    "collect stats" in {
      val stats = new FakeStatsCollector
      val statsCollectingQuery = new StatsCollectingQuery(testQuery, stats)

      statsCollectingQuery.select { _ => 1 } mustEqual List(1)

      stats.counts("db-select-count") mustEqual 1
      stats.times("db-timing") mustEqual latency.inMillis
    }
  }
}

