package com.twitter.querulous.integration

import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, SimplePoolingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.util.TimeConversions._

object SimpleJdbcPoolSpec {
  val testDatabaseFactory = new SimplePoolingDatabaseFactory(1, 1.second, 1.second, 1.second, Map.empty)
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
  private val userEnv = System.getenv("DB_USERNAME")
  private val passEnv = System.getenv("DB_PASSWORD")
}

class SimpleJdbcPoolSpec extends ConfiguredSpecification {
  import SimpleJdbcPoolSpec._

  val queryEvaluator = testEvaluatorFactory(config)

  "SimpleJdbcPoolSpec" should {
    "execute some queries" >> {
      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      queryEvaluator.select("SELECT 2 FROM DUAL") { r => r.getInt(1) } mustEqual List(2)
    }

    "timeout when attempting to get a second connection" >> {
      queryEvaluator.select("SELECT 1 FROM DUAL") { r =>
        queryEvaluator.select("SELECT 2 FROM DUAL") { r2 => } must throwA[SqlDatabaseTimeoutException]
      }
    }
  }
}

