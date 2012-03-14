package com.twitter.querulous

import com.twitter.querulous.database.SingleConnectionDatabaseFactory
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.util.Eval
import java.io.File
import java.util.concurrent.CountDownLatch
import org.specs.Specification

import config.Connection

trait ConfiguredSpecification extends Specification {
  val config = try {
    val eval = new Eval
    eval[Connection](new File("config/test.scala"))
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }
}

object TestEvaluator {
//  val testDatabaseFactory = new MemoizingDatabaseFactory()
  val testDatabaseFactory = new SingleConnectionDatabaseFactory
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)

  private val userEnv = System.getenv("DB_USERNAME")
  private val passEnv = System.getenv("DB_PASSWORD")

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
