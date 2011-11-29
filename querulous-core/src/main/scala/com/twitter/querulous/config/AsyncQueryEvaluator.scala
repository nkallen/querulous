package com.twitter.querulous.config

import com.twitter.util
import com.twitter.querulous
import com.twitter.querulous.async

trait FuturePool {
  def apply(): util.FuturePool
}

class AsyncQueryEvaluator {
  var workPool: FuturePool = new FuturePool {
    def apply() = async.AsyncQueryEvaluator.defaultWorkPool
  }

  var checkoutPool: FuturePool = new FuturePool {
    def apply() = async.AsyncQueryEvaluator.checkoutPool(maxWaiters)
  }

  var database: Database     = new Database
  var query: Query           = new Query
  var maxWaiters             = async.AsyncQueryEvaluator.defaultMaxWaiters
  var singletonFactory       = false

  private var memoizedFactory: Option[async.AsyncQueryEvaluatorFactory] = None

  protected def newQueryFactory(stats: querulous.StatsCollector) = {
    query(stats)
  }

  protected def newDatabaseFactory(stats: querulous.StatsCollector) = {
    database(stats)
  }

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = {
    synchronized {
      if (!singletonFactory) memoizedFactory = None

      memoizedFactory = memoizedFactory orElse {
        val db = new async.BlockingDatabaseWrapperFactory(
          workPool(),
          checkoutPool(),
          newDatabaseFactory(stats),
          stats
        )

        Some(new async.StandardAsyncQueryEvaluatorFactory(db, newQueryFactory(stats)))
      }

      memoizedFactory.get
    }
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)
}
