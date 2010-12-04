package com.twitter.querulous.config

import net.lag.logging.Logger
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import query._

trait RetryingQuery {
  def retries: Int
}

object QueryTimeout {
  def apply(timeout: Duration, cancelOnTimeout: Boolean) =
    new QueryTimeout(timeout, cancelOnTimeout)

  def apply(timeout: Duration) =
    new QueryTimeout(timeout, false)
}

class QueryTimeout(val timeout: Duration, val cancelOnTimeout: Boolean)

trait Query {
  var timeouts: Map[QueryClass, QueryTimeout] = Map(
    QueryClass.Select -> QueryTimeout(5.seconds),
    QueryClass.Execute -> QueryTimeout(5.seconds)
  )

  var retry: Option[RetryingQuery] = None
  def retry_=(r: RetryingQuery) { retry = Some(r) }
  var debug: Option[(String => Unit)] = None
  def debug_=(d: String => Unit) { debug = Some(d) }

  def apply(statsCollector: StatsCollector): QueryFactory = {
    var queryFactory: QueryFactory = new SqlQueryFactory

    if (!timeouts.isEmpty) {
      val tupleTimeout = Map(timeouts.map { case (queryClass, timeout) =>
        (queryClass, (timeout.timeout, timeout.cancelOnTimeout))
      }.toList: _*)

      queryFactory = new PerQueryTimingOutQueryFactory(new SqlQueryFactory, tupleTimeout)
    }

    if (statsCollector ne NullStatsCollector) {
      queryFactory = new StatsCollectingQueryFactory(queryFactory, statsCollector)
    }

    retry.foreach { retryConfig =>
      queryFactory = new RetryingQueryFactory(queryFactory, retryConfig.retries)
    }

    debug.foreach{ logger =>
      queryFactory = new DebuggingQueryFactory(queryFactory, logger)
    }

    queryFactory
  }

  def apply(): QueryFactory = apply(NullStatsCollector)
}
