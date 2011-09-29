package com.twitter.querulous.config

import com.twitter.util
import com.twitter.querulous
import com.twitter.querulous.async

trait FuturePool {
  def apply(): util.FuturePool
}

object DefaultFuturePool extends FuturePool {
  def apply() = async.AsyncQueryEvaluator.defaultFuturePool
}

class AsyncQueryEvaluator {
  var futurePool: FuturePool = DefaultFuturePool
  var database: Database     = new Database
  var query: Query           = new Query
  var maxWaiters             = async.AsyncQueryEvaluator.defaultMaxWaiters

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = {
    val db = new async.BlockingDatabaseWrapperFactory(
      futurePool(),
      async.AsyncQueryEvaluator.checkoutPool(maxWaiters),
      database(stats)
    )

    new async.StandardAsyncQueryEvaluatorFactory(db, query(stats))
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)
}
