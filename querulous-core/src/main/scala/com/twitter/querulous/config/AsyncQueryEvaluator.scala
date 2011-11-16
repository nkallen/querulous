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

  def apply(stats: querulous.StatsCollector): async.AsyncQueryEvaluatorFactory = {
    synchronized {
      if (!singletonFactory) memoizedFactory = None

      val workP     = workPool()
      val checkoutP = checkoutPool()

      getExecutor(workP) foreach { e =>
        stats.addGauge("db-async-active-threads")(e.getActiveCount.toDouble)
      }

      getExecutor(checkoutP) foreach { e =>
        val q = e.getQueue
        stats.addGauge("db-async-waiters")(q.size.toDouble)
      }

      memoizedFactory = memoizedFactory orElse {
        val db = new async.BlockingDatabaseWrapperFactory(
          workP,
          checkoutP,
          database(stats)
        )

        Some(new async.StandardAsyncQueryEvaluatorFactory(db, query(stats)))
      }

      memoizedFactory.get
    }
  }

  def apply(): async.AsyncQueryEvaluatorFactory = apply(querulous.NullStatsCollector)

  private def getExecutor(p: util.FuturePool) = p match {
    case p: util.ExecutorServiceFuturePool => p.executor match {
      case e: java.util.concurrent.ThreadPoolExecutor => Some(e)
      case _ => None
    }
    case _ => None
  }
}
