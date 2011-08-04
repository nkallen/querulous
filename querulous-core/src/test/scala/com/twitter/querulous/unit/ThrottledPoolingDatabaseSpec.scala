package com.twitter.querulous.unit

import com.twitter.conversions.time._
import org.apache.commons.pool.ObjectPool
import org.specs.Specification
import org.specs.mock.JMocker
import org.specs.util.TimeConversions
import com.twitter.querulous.database._
import java.sql.{SQLException, Connection}

class PooledConnectionSpec extends Specification with JMocker {
  "PooledConnectionSpec" should {
    val p = mock[ObjectPool]
    val c = mock[Connection]

    "return to the pool" in {
      val conn = new PooledConnection(c, p)

      expect {
        one(c).isClosed() willReturn false
        one(p).returnObject(conn)
      }

      conn.close()
    }

    "eject from the pool only once" in {
      val conn = new PooledConnection(c, p)

      expect {
        one(c).isClosed() willReturn true
        one(p).invalidateObject(conn)
        one(c).isClosed() willReturn true
      }

      conn.close() must throwA[SQLException]
      conn.close() must throwA[SQLException]
    }
  }
}

class ThrottledPoolSpec extends Specification with JMocker {
  "ThrottledPoolSpec" should {
    val connection = mock[Connection]

    val repopulateInterval = 250.millis
    val idleTimeout = 50.millis
    def createPool(size: Int) = { new ThrottledPool( { () => connection }, size, 10.millis, 50.millis) }

    "create and populate" in {
      val pool = createPool(5)
      pool.getTotal() mustEqual 5
    }

    "checkout" in {
      val pool = createPool(5)
      pool.getTotal() mustEqual 5
      pool.borrowObject()
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
      pool.borrowObject()
      pool.getNumIdle() mustEqual 0
      pool.borrowObject() must throwA[PoolTimeoutException]
    }

    "fast fail when the pool is empty" {
      val pool = createPool(0)
      pool.getTotal() mustEqual 0
      pool.borrowObject() must throwA[PoolEmptyException]
      1
    }

    "eject idle" in {
      expect {
        one(connection).isClosed() willReturn true
        one(connection).isClosed() willReturn true
      }

      val pool = createPool(1)
      pool.getTotal() mustEqual 1
      Thread.sleep(idleTimeout.inMillis + 5)
      pool.borrowObject()
      pool.getNumIdle() mustEqual 0
      pool.getTotal() mustEqual 1

      // we should throw a timeout exception when the pool isn't empty.
      pool.addObject()
      pool.getTotal() mustEqual 2
      Thread.sleep(idleTimeout.inMillis + 5)
      pool.borrowObject() must throwA[PoolTimeoutException]
    }

    "repopulate" in {
      val pool = createPool(2)
      val conn = pool.borrowObject()
      pool.invalidateObject(conn)
      pool.getTotal() mustEqual 1
      val conn2 = pool.borrowObject()
      pool.invalidateObject(conn2)
      pool.getTotal() mustEqual 0
      new PoolWatchdogThread(pool, List(""), repopulateInterval).start()
      // 4 retries give us about 300ms to check whether the condition is finally met
      // because first check is applied immediately
      pool.getTotal() must eventually(4, TimeConversions.intToRichLong(100).millis) (be_==(1))
      pool.getTotal() must eventually(4, TimeConversions.intToRichLong(100).millis) (be_==(2))
      Thread.sleep(repopulateInterval.inMillis + 100)
      // make sure that the watchdog thread won't add more connections than the size of the pool
      pool.getTotal() must be_==(2)
    }
  }
}
