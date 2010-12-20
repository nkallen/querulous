package com.twitter.querulous.unit

import scala.collection.mutable.Map
import java.sql.Connection
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.querulous.database.StatsCollectingDatabase
import com.twitter.querulous.test.{FakeStatsCollector, FakeDatabase}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._


class StatsCollectingDatabaseSpec extends Specification with JMocker with ClassMocker {
  "StatsCollectingDatabase" should {
    val latency = 1.second
    val connection = mock[Connection]
    val stats = new FakeStatsCollector
    def pool(callback: String => Unit) = new StatsCollectingDatabase(new FakeDatabase(connection, callback), stats)

    "collect stats" in {
      "when closing" >> {
        Time.withCurrentTimeFrozen { time =>
          pool(s => time.advance(latency)).close(connection)
          stats.times("database-close-timing") mustEqual latency.inMillis
        }
      }

      "when opening" >> {
        Time.withCurrentTimeFrozen { time =>
          pool(s => time.advance(latency)).open()
          stats.times("database-open-timing") mustEqual latency.inMillis
        }
      }
    }
  }
}
