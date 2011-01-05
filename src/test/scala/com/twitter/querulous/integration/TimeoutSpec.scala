package com.twitter.querulous.integration

import java.util.concurrent.CountDownLatch
import org.specs.Specification
import org.specs.mock.{JMocker, ClassMocker}
import net.lag.configgy.Configgy
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.querulous.{TestEvaluator, Timeout, TimeoutException}
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.query.{SqlQueryFactory, TimingOutQueryFactory, SqlQueryTimeoutException}
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}


class TimeoutSpec extends Specification with JMocker with ClassMocker {
  Configgy.configure("config/test.conf")

  import TestEvaluator._

  val config = Configgy.config.configMap("db")
  val username = config("username")
  val password = config("password")
  val timeout = 1.second
  val timingOutQueryFactory = new TimingOutQueryFactory(testQueryFactory, timeout, false)
  val apacheDatabaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
  val timingOutQueryEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, timingOutQueryFactory)

  "Timeouts" should {
    doBefore {
      testEvaluatorFactory("localhost", null, username, password).execute("CREATE DATABASE IF NOT EXISTS db_test")
    }

    "honor timeouts" in {
      val queryEvaluator1 = testEvaluatorFactory(List("localhost"), "db_test", username, password)
      val dbLock = getDbLock(queryEvaluator1, "padlock")

      val thread = new Thread() {
        override def run() {
          try {
            Thread.sleep(60.seconds.inMillis)
          } catch { case _ => () }
          dbLock.countDown()
        }
      }
      thread.start()

      val queryEvaluator2 = timingOutQueryEvaluatorFactory(List("localhost"), "db_test", username, password)
      val start = Time.now
      queryEvaluator2.select("SELECT GET_LOCK('padlock', 60) AS rv") { row => row.getInt("rv") } must throwA[SqlQueryTimeoutException]
      val end = Time.now
      (end - start).inMillis must beCloseTo(timeout.inMillis, 1.second.inMillis)
      thread.interrupt()
      thread.join()
    }

    "purge intermittently" in {
      val task = capturingParam[java.util.TimerTask]

      class FakeTimer extends java.util.Timer  {
        var numPurges = 0

        // Ignore the timeout and run the task immediately, to force the appearance of a timeout.
        override def schedule(task: java.util.TimerTask, timeout: Long) = task.run

        override def purge = { numPurges += 1; 0}
      }

      val fakeTimer = new FakeTimer()
      val fakeTimeout = new Timeout(fakeTimer, 3)

      def forceTimeout(shouldPurge: Boolean) {
        val purgesBefore = fakeTimer.numPurges
        fakeTimeout(1.second)(1)(()) must throwA[TimeoutException]
        val purgesAfter = fakeTimer.numPurges
        shouldPurge == (purgesAfter != purgesBefore) must beTrue
      }

      forceTimeout(false)
      forceTimeout(false)
      forceTimeout(true)
      forceTimeout(false)
      forceTimeout(false)
      forceTimeout(true)
    }
  }
}
