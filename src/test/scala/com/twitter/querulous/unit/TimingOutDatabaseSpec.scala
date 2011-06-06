package com.twitter.querulous.unit

import com.twitter.querulous._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.sql.Connection
import com.twitter.querulous.TimeoutException
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, TimingOutDatabase}
import com.twitter.querulous.test.FakeDatabase
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}


class TimingOutDatabaseSpec extends Specification with JMocker with ClassMocker {
  "TimingOutDatabaseSpec" should {
    val latch = new CountDownLatch(1)
    val timeout = 1.second
    var shouldWait = false
    val connection = mock[Connection]
    val future = new FutureTimeout(1, 1)
    val database = new FakeDatabase {
      def open() = {
        if (shouldWait) latch.await(100.seconds.inMillis, TimeUnit.MILLISECONDS)
        connection
      }
      def close(connection: Connection) = ()
    }

    expect {
//      one(connection).close()
    }

    val timingOutDatabase = new TimingOutDatabase(database, future, timeout)
    shouldWait = true

    "timeout" in {
      try {
        val epsilon = 150.millis
        var start = Time.now
        timingOutDatabase.open() must throwA[SqlDatabaseTimeoutException]
        var end = Time.now
        (end.inMillis - start.inMillis) must beCloseTo(timeout.inMillis, epsilon.inMillis)
      } finally {
        latch.countDown()
      }
    }
  }
}
