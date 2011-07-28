package com.twitter.querulous.unit

import scala.collection.mutable.Map
import java.sql.Connection
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, StatsCollectingDatabase}
import com.twitter.querulous.test.{FakeStatsCollector, FakeDBConnectionWrapper}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._


class StatsCollectingDatabaseSpec extends Specification with JMocker with ClassMocker {
  "StatsCollectingDatabase" should {
    val latency = 1.second
    val connection = mock[Connection]
    val stats = new FakeStatsCollector
    def pool(callback: String => Unit) = new StatsCollectingDatabase(
      new FakeDBConnectionWrapper(connection, callback),
      "test",
      stats
    )

    "collect stats" in {
      "when closing" >> {
        Time.withCurrentTimeFrozen { time =>
          pool(s => time.advance(latency)).close(connection)
          stats.times("db-close-timing") mustEqual latency.inMillis
        }
      }

      "when opening" >> {
        Time.withCurrentTimeFrozen { time =>
          pool(s => time.advance(latency)).open()
          stats.times("db-open-timing") mustEqual latency.inMillis
        }
      }
    }

    "collect timeout stats" in {
      val e = new SqlDatabaseTimeoutException("foo", 0.seconds)
      "when closing" >> {
        pool(s => throw e).close(connection) must throwA[SqlDatabaseTimeoutException]
        stats.counts("db-close-timeout-count") mustEqual 1
        stats.counts("db-test-close-timeout-count") mustEqual 1
      }

      "when opening" >> {
        Time.withCurrentTimeFrozen { time =>
          pool(s => throw e).open() must throwA[SqlDatabaseTimeoutException]
          stats.counts("db-open-timeout-count") mustEqual 1
          stats.counts("db-test-open-timeout-count") mustEqual 1
        }
      }
    }
  }
}
