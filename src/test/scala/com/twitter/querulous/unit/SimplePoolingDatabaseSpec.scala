package com.twitter.querulous.unit

import com.twitter.querulous.database.{PoolTimeoutException, SimplePool}
import com.twitter.util.TimeConversions._
import java.sql.Connection
import org.specs.Specification
import org.specs.mock.JMocker

class SimpleJdbcPoolSpec extends Specification with JMocker {
  "SimplePoolingDatabaseSpec" should {
    val size = 1
    val connection = mock[Connection]

    def createPool(size: Int) = { new SimplePool( { () => connection }, size, 20.millis) }

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
  }
}
