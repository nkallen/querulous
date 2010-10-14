package com.twitter.querulous

import java.util.concurrent.CountDownLatch
import net.lag.configgy.Configgy
import com.twitter.querulous.database.{MemoizingDatabaseFactory, SingleConnectionDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


object TestEvaluator {
//  val testDatabaseFactory = new MemoizingDatabaseFactory()
  val testDatabaseFactory = new SingleConnectionDatabaseFactory
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)


  def getDbLock(queryEvaluator: QueryEvaluator, lockName: String) = {
    val returnLatch = new CountDownLatch(1)
    val releaseLatch = new CountDownLatch(1)

    val thread = new Thread() {
      override def run() {
        queryEvaluator.select("SELECT GET_LOCK('" + lockName + "', 1) AS rv") { row =>
          returnLatch.countDown()
          try {
            releaseLatch.await()
          } catch {
            case _ =>
          }
        }
      }
    }

    thread.start()
    returnLatch.await()

    releaseLatch
  }
}
