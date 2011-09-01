package com.twitter.querulous.integration

import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.database.{SqlDatabaseTimeoutException, ThrottledPoolingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.conversions.time._

object ThrottledPoolingDatabaseSpec {
  val testDatabaseFactory = new ThrottledPoolingDatabaseFactory(1, 1.second, 1.second, 1.second, Map.empty)
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
}

class ThrottledPoolingDatabaseSpec extends ConfiguredSpecification {
  import ThrottledPoolingDatabaseSpec._

  val queryEvaluator = testEvaluatorFactory(config)

  "ThrottledJdbcPoolSpec" should {
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
