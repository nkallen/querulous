package com.twitter.querulous.integration

import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.database.SimplePoolingDatabaseFactory
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.util.TimeConversions._

object SimpleJdbcPoolSpec {
  val testDatabaseFactory = new SimplePoolingDatabaseFactory(1, 1.second, 1.second, Map())
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
    }
  }
}

