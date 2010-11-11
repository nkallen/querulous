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
  def timeouts: Map[QueryClass, QueryTimeout] = Map(
    QueryClass.Select -> QueryTimeout(5.seconds),
    QueryClass.Execute -> QueryTimeout(5.seconds)
  )

  def retry: Option[RetryingQuery] = None
  def debug: Option[(String => Unit)] = None

  def apply(statsCollector: StatsCollector): QueryFactory = {
    val tupleTimeout = Map(timeouts.map { case (queryClass, timeout) =>
      (queryClass, (timeout.timeout, timeout.cancelOnTimeout))
    }.toList: _*)

    var queryFactory: QueryFactory =
      new PerQueryTimingOutQueryFactory(new SqlQueryFactory, tupleTimeout)

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
