package com.twitter.querulous.integration

import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.query.{TimingOutQueryFactory, SqlQueryTimeoutException}
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory}
import com.twitter.querulous.ConfiguredSpecification


class TimeoutSpec extends ConfiguredSpecification {
  import TestEvaluator._

  val timeout = 1.second
  val timingOutQueryFactory = new TimingOutQueryFactory(testQueryFactory, timeout, false)
  val apacheDatabaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
  val timingOutQueryEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, timingOutQueryFactory)

  "Timeouts" should {
    doBefore {
      testEvaluatorFactory(config.withoutDatabase).execute("CREATE DATABASE IF NOT EXISTS db_test")
    }

    "honor timeouts" in {
      val queryEvaluator1 = testEvaluatorFactory(config)
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

      val queryEvaluator2 = timingOutQueryEvaluatorFactory(config)
      val start = Time.now
      queryEvaluator2.select("SELECT GET_LOCK('padlock', 60) AS rv") { row => row.getInt("rv") } must throwA[SqlQueryTimeoutException]
      val end = Time.now
      (end - start).inMillis must beCloseTo(timeout.inMillis, 1.second.inMillis)
      thread.interrupt()
      thread.join()
    }
  }
}
