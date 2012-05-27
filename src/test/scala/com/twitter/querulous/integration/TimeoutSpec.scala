package com.twitter.querulous.integration

import java.util.concurrent.CountDownLatch
import org.specs.Specification
import net.lag.configgy.Configgy
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.query.{SqlQueryFactory, TimingOutQueryFactory, SqlQueryTimeoutException}
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}


class TimeoutSpec extends Specification {
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
  }
}
