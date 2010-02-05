package com.twitter.querulous.unit

import java.sql.Connection
import scala.collection.mutable.Map
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.querulous.connectionpool.StatsCollectingConnectionPool
import com.twitter.querulous.fake.{FakeStatsCollector, FakeConnectionPool}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._

object StatsCollectingConnectionPoolSpec extends Specification with JMocker {
  "StatsCollectingConnectionPool" should {
    Time.freeze()
    val latency = 1.second
    val connection = mock[Connection]
    val stats = new FakeStatsCollector
    val pool = new StatsCollectingConnectionPool(new FakeConnectionPool(connection, latency), stats)

    "collect stats" in {
      "when releasing" >> {
        pool.release(connection)
        stats.times("connection-pool-release-timing") mustEqual latency.inMillis
      }

      "when reserving" >> {
        pool.reserve()
        stats.times("connection-pool-reserve-timing") mustEqual latency.inMillis
      }
    }
  }
}
