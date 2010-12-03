package com.twitter.querulous.config

import com.twitter.util.Duration
import evaluator._

trait AutoDisablingQueryEvaluator {
  def errorCount: Int
  def interval: Duration
}

trait QueryEvaluator {
  def database: Database
  def query: Query

  def autoDisable: Option[AutoDisablingQueryEvaluator]

  def apply(stats: StatsCollector) = {
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
