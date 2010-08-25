package com.twitter.querulous

import net.lag.configgy.Configgy
import com.twitter.querulous.database.{MemoizingDatabaseFactory, SingleConnectionDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.util.Time
import com.twitter.util.TimeConversions._


object TestEvaluator {
//  val testDatabaseFactory = new MemoizingDatabaseFactory()
  val testDatabaseFactory = new SingleConnectionDatabaseFactory
  val testQueryFactory = new SqlQueryFactory
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
}
