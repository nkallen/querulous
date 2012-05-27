package com.twitter.querulous.config

import com.twitter.util
import com.twitter.querulous
import com.twitter.querulous.async
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.query.QueryFactory

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

  // Optionally takes in a method to transform the QueryFactory we are going to use (typically used for stats collection).
  protected def newQueryFactory(stats: querulous.StatsCollector, queryStatsFactory: Option[QueryFactory => QueryFactory]) = {
    query(stats, queryStatsFactory)
  }

  // Optionally takes in a method to transform the DatabaseFactory we are going to use (typically used for stats collection).
  protected def newDatabaseFactory(stats: querulous.StatsCollector, dbStatsFactory: Option[DatabaseFactory => DatabaseFactory]) = {
    database(stats, dbStatsFactory)
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = apply(stats, None, None)

  def apply(stats: querulous.StatsCollector, dbStatsFactory: DatabaseFactory => DatabaseFactory, queryStatsFactory: QueryFactory => QueryFactory): async.AsyncQueryEvaluatorFactory = apply(stats, Some(dbStatsFactory), Some(queryStatsFactory))

  def apply(stats: querulous.StatsCollector, dbStatsFactory: Option[DatabaseFactory => DatabaseFactory], queryStatsFactory: Option[QueryFactory => QueryFactory]): async.AsyncQueryEvaluatorFactory = {
    synchronized {
      if (!singletonFactory) memoizedFactory = None

      memoizedFactory = memoizedFactory orElse {
        val db = new async.BlockingDatabaseWrapperFactory(
          workPool(),
          checkoutPool(),
          newDatabaseFactory(stats, dbStatsFactory),
          stats
        )

        Some(new async.StandardAsyncQueryEvaluatorFactory(db, newQueryFactory(stats, queryStatsFactory)))
      }

      memoizedFactory.get
    }
  }
}
