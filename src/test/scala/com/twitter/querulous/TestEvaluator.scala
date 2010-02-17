package com.twitter.querulous

import net.lag.configgy.Configgy
import com.twitter.querulous.database.{MemoizingDatabaseFactory, ApachePoolingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._

object TestEvaluator {
  val testDatabaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds))
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
}



