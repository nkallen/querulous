package com.twitter.querulous.config

import com.twitter.querulous._
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.query.QueryFactory
import com.twitter.util.Duration
import evaluator._

trait AutoDisablingQueryEvaluator {
  def errorCount: Int
  def interval: Duration
}

class QueryEvaluator {
  var database: Database = new Database
  var query: Query = new Query

  var autoDisable: Option[AutoDisablingQueryEvaluator] = None
  def autoDisable_=(a: AutoDisablingQueryEvaluator) { autoDisable = Some(a) }

  def apply(stats: StatsCollector): QueryEvaluatorFactory = apply(stats, None, None)
  def apply(stats: StatsCollector, dbStatsFactory: DatabaseFactory => DatabaseFactory, queryStatsFactory: QueryFactory => QueryFactory): QueryEvaluatorFactory = apply(stats, Some(dbStatsFactory), Some(queryStatsFactory))
  def apply(stats: StatsCollector, dbStatsFactory: Option[DatabaseFactory => DatabaseFactory], queryStatsFactory: Option[QueryFactory => QueryFactory]): QueryEvaluatorFactory = {
    var factory: QueryEvaluatorFactory =
      new StandardQueryEvaluatorFactory(database(stats, dbStatsFactory), query(stats, queryStatsFactory))

    autoDisable.foreach { disable =>
      factory = new AutoDisablingQueryEvaluatorFactory(
        factory, disable.errorCount, disable.interval
      )
    }
    factory
  }

  def apply(): QueryEvaluatorFactory = apply(NullStatsCollector)
}
