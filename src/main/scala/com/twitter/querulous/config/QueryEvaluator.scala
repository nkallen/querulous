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

  def apply() = {
    var factory: QueryEvaluatorFactory = new StandardQueryEvaluatorFactory(database(), query())
    autoDisable.foreach { disableConfig =>
      factory = new AutoDisablingQueryEvaluatorFactory(factory,
                                                       disableConfig.errorCount,
                                                       disableConfig.interval)
    }
    factory
  }
}
