package com.twitter.querulous.unit

import java.sql.ResultSet
import net.lag.configgy.Configgy
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.test.FakeQuery
import com.twitter.querulous.query.{TimingOutQuery, SqlQueryTimeoutException}
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import java.util.concurrent.{CountDownLatch, TimeUnit}


class TimingOutQuerySpec extends Specification with JMocker with ClassMocker {
  Configgy.configure("config/test.conf")

  "TimingOutQuery" should {
    val config = Configgy.config.configMap("db")
    val connection = TestEvaluator.testDatabaseFactory(List("localhost"), config("username"), config("password")).open()
    val timeout = 1.second
    val cancelTimeout = 30.millis
    val resultSet = mock[ResultSet]

    "timeout" in {
      val latch = new CountDownLatch(1)
      val query = new FakeQuery(List(resultSet)) {
        override def cancel() = { latch.countDown() }

        override def select[A](f: ResultSet => A) = {
          latch.await(2.second.inMillis, TimeUnit.MILLISECONDS)
          super.select(f)
        }
      }
      val timingOutQuery = new TimingOutQuery(query, connection, timeout, cancelTimeout)

      timingOutQuery.select { r => 1 } must throwA[SqlQueryTimeoutException]
      latch.getCount mustEqual 0
    }

    "not timeout" in {
      val latch = new CountDownLatch(1)
      val query = new FakeQuery(List(resultSet)) {
        override def cancel() = { latch.countDown() }
      }
      val timingOutQuery = new TimingOutQuery(query, connection, timeout, cancelTimeout)

      timingOutQuery.select { r => 1 }
      latch.getCount mustEqual 1
    }
  }
}
