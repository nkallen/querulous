package com.twitter.querulous.config

import com.twitter.querulous._
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


  def apply(stats: StatsCollector): QueryEvaluatorFactory = {
    var factory: QueryEvaluatorFactory =
      new StandardQueryEvaluatorFactory(database(stats), query(stats))

    autoDisable.foreach { disable =>
      factory = new AutoDisablingQueryEvaluatorFactory(
        factory, disable.errorCount, disable.interval
      )
    }
    factory
  }

  def apply(): QueryEvaluatorFactory = apply(NullStatsCollector)
}
