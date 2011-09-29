package com.twitter.querulous.config

import com.twitter.util
import com.twitter.querulous
import com.twitter.querulous.async

trait FuturePool {
  def apply(): util.FuturePool
}

object DefaultWorkPool extends FuturePool {
  def apply() = async.AsyncQueryEvaluator.defaultWorkPool
}

class AsyncQueryEvaluator {
  var workPool: FuturePool = DefaultWorkPool
  var database: Database     = new Database
  var query: Query           = new Query
  var maxWaiters             = async.AsyncQueryEvaluator.defaultMaxWaiters

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = {
    val db = new async.BlockingDatabaseWrapperFactory(
      DefaultWorkPool(),
      async.AsyncQueryEvaluator.checkoutPool(maxWaiters),
      database(stats)
    )

    new async.StandardAsyncQueryEvaluatorFactory(db, query(stats))
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)
}
