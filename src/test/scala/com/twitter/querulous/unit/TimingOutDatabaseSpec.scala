package com.twitter.querulous.unit

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.sql.Connection
import com.twitter.querulous.TimeoutException
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, TimingOutDatabase, Database}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}


class TimingOutDatabaseSpec extends Specification with JMocker with ClassMocker {
  "TimingOutDatabaseSpec" should {
    Time.reset()
    val latch = new CountDownLatch(1)
    val timeout = 1.second
    var shouldWait = false
    val connection = mock[Connection]
    val future = new FutureTimeout(1, 1)
    val database = new Database {
      def open() = {
        if (shouldWait) latch.await(100.seconds.inMillis, TimeUnit.MILLISECONDS)
        connection
      }
      def close(connection: Connection) = ()
    }

    expect {
//      one(connection).close()
    }

    val timingOutDatabase = new TimingOutDatabase(database, List("dbhost"), "dbname", future, timeout, 1)
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
