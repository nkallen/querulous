package com.twitter.querulous.integration

import java.util.concurrent.CountDownLatch
import org.specs.Specification
import net.lag.configgy.Configgy
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.query.{SqlQueryFactory, TimingOutQueryFactory, SqlTimeoutException}
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}

object TimeoutSpec extends Specification {
  val config = Configgy.config.configMap("db")
  val username = config("username")
  val password = config("password")
  val timeout = 1.second
  val queryFactory = new SqlQueryFactory
  val timingOutQueryFactory = new TimingOutQueryFactory(queryFactory, timeout)
  val databaseFactory = new ApachePoolingDatabaseFactory(1, 1, 1.second, 20.millis, false, 0.seconds)
  val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
  val timingOutQueryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, timingOutQueryFactory)

  "Timeouts" should {
    doBefore {
      QueryEvaluator("localhost", null, username, password).execute("CREATE DATABASE IF NOT EXISTS db_test")
    }

    "honor timeouts" in {
      val queryEvaluator1 = QueryEvaluator(List("localhost"), "db_test", username, password)
      val latch = new CountDownLatch(1)
      val thread = new Thread() {
        override def run() {
          queryEvaluator1.select("SELECT GET_LOCK('padlock', 1) AS rv") { row =>
            latch.countDown()
            try {
              Thread.sleep(60.seconds.inMillis)
            } catch {
              case _ =>
            }
          }
        }
      }
      thread.start()
      latch.await()

      val queryEvaluator2 = timingOutQueryEvaluatorFactory(List("localhost"), "db_test", username, password)
      val start = Time.now
      queryEvaluator2.select("SELECT GET_LOCK('padlock', 60) AS rv") { row => row.getInt("rv") } must throwA[SqlTimeoutException]
      val end = Time.now
      (end - start).inMillis must beCloseTo(timeout.inMillis, 1.second.inMillis)
      thread.interrupt()
      thread.join()
    }
  }
}
