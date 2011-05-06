package com.twitter.querulous.unit

import com.twitter.querulous.database.{PoolTimeoutException, SimplePool, PoolWatchdog}
import com.twitter.util.TimeConversions._
import java.sql.Connection
import org.specs.Specification
import org.specs.mock.JMocker

class SimpleJdbcPoolSpec extends Specification with JMocker {
  "SimplePoolingDatabaseSpec" should {
    val size = 1
    val connection = mock[Connection]

    val repopulateInterval = 250.millis
    val idleTimeout = 50.millis
    def createPool(size: Int) = { new SimplePool( { () => connection }, size, 10.millis, 50.millis) }

    "create and populate" in {
      val pool = createPool(5)
      pool.getTotal() mustEqual 5
    }

    "checkout" in {
      val pool = createPool(5)
      pool.getTotal() mustEqual 5
      val conn = pool.borrowObject()
      pool.getNumActive() mustEqual 1
      pool.getNumIdle() mustEqual 4
    }

    "return" in {
      val pool = createPool(5)
      pool.getTotal() mustEqual 5
      val conn = pool.borrowObject()
      pool.getNumActive() mustEqual 1
      pool.returnObject(conn)
      pool.getNumActive() mustEqual 0
      pool.getNumIdle() mustEqual 5
    }

    "timeout" in {
      val pool = createPool(1)
      pool.getTotal() mustEqual 1
      val conn = pool.borrowObject()
      pool.getNumIdle() mustEqual 0
      pool.borrowObject() must throwA[PoolTimeoutException]
    }

    "eject idle" in {
      expect {
        one(connection).close()
        one(connection).close()
      }

      val pool = createPool(1)
      pool.getTotal() mustEqual 1
      Thread.sleep(idleTimeout.inMillis + 5)
      val conn = pool.borrowObject() mustNot throwA[PoolTimeoutException]
      pool.getNumIdle() mustEqual 0
      pool.getTotal() mustEqual 1

      // we should throw a timeout exception when the pool isn't empty.
      pool.addObject()
      pool.getTotal() mustEqual 2
      Thread.sleep(idleTimeout.inMillis + 5)
      pool.borrowObject() must throwA[PoolTimeoutException]
    }

    "repopulate" in {
      val pool = createPool(1)
      val watchdog = new PoolWatchdog(pool, repopulateInterval, "test-watchdog-thread")
      val conn = pool.borrowObject()
      pool.invalidateObject(conn)
      pool.getTotal() mustEqual 0
      watchdog.start()
      Thread.sleep(repopulateInterval)
      pool.getTotal() must eventually(be_==(1))
      watchdog.stop()
    }
  }
}
