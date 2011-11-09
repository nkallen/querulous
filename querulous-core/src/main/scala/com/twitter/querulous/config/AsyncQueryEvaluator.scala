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
  var singletonFactory       = false

  private var memoizedFactory: Option[async.AsyncQueryEvaluatorFactory] = None

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = {
    synchronized {
      if (!singletonFactory) memoizedFactory = None

      memoizedFactory = memoizedFactory orElse {
        val db = new async.BlockingDatabaseWrapperFactory(
          DefaultWorkPool(),
          async.AsyncQueryEvaluator.checkoutPool(maxWaiters),
          database(stats)
        )

        Some(new async.StandardAsyncQueryEvaluatorFactory(db, query(stats)))
      }

      memoizedFactory.get
    }
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)
}
