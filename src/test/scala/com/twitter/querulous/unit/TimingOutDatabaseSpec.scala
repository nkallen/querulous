package com.twitter.querulous.unit

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.sql.Connection
import com.twitter.querulous.TimeoutException
import com.twitter.querulous.database.{Database, TimingOutDatabase}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}

object TimingOutDatabaseSpec extends Specification with JMocker with ClassMocker {
  "TimingOutDatabaseSpec" should {
    Time.reset()
    val latch = new CountDownLatch(1)
    val timeout = 1.second
    val connection = mock[Connection]
    val database = new Database {
      def open() = {
        latch.await(100.seconds.inMillis, TimeUnit.MILLISECONDS)
        connection
      }
      def close(connection: Connection) = ()
    }
    val timingOutDatabase = new TimingOutDatabase(database, 1, timeout)

    "timeout" in {
      try {
        val epsilon = 100.millis
        var start = Time.now
        timingOutDatabase.open() must throwA[TimeoutException]
        var end = Time.now
        (end.inMillis - start.inMillis) must beCloseTo(timeout.inMillis, epsilon.inMillis)

        // subsequent failures should be instantaneous:
        start = Time.now
        timingOutDatabase.open() must throwA[TimeoutException]
        end = Time.now
        (end.inMillis - start.inMillis) must beCloseTo(0L, epsilon.inMillis)
      } finally {
        latch.countDown()
      }
    }

  }
}


