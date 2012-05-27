package com.twitter.querulous.unit

import java.sql.ResultSet
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.test.FakeQuery
import com.twitter.querulous.query.{TimingOutQuery, SqlQueryTimeoutException}
import com.twitter.querulous.ConfiguredSpecification
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import java.util.concurrent.{CountDownLatch, TimeUnit}


object TimingOutQuerySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "TimingOutQuery" should {
    val connection = TestEvaluator.testDatabaseFactory(
      config.hostnames.toList, config.username, config.password).open()
    val timeout = 1.second
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
      val timingOutQuery = new TimingOutQuery(query, connection, timeout, true)

      timingOutQuery.select { r => 1 } must throwA[SqlQueryTimeoutException]
      latch.getCount mustEqual 0
    }

    "not timeout" in {
      val latch = new CountDownLatch(1)
      val query = new FakeQuery(List(resultSet)) {
        override def cancel() = { latch.countDown() }
      }
      val timingOutQuery = new TimingOutQuery(query, connection, timeout, true)

      timingOutQuery.select { r => 1 }
      latch.getCount mustEqual 1
    }
  }
}
