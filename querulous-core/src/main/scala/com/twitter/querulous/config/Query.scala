package com.twitter.querulous.config

import com.twitter.querulous._
import com.twitter.util.Duration
import com.twitter.conversions.time._
import query._


object QueryTimeout {
  def apply(timeout: Duration, cancelOnTimeout: Boolean) =
    new QueryTimeout(timeout, cancelOnTimeout)

  def apply(timeout: Duration) =
    new QueryTimeout(timeout, false)
}

class QueryTimeout(val timeout: Duration, val cancelOnTimeout: Boolean)

object NoDebugOutput extends (String => Unit) {
  def apply(s: String) = ()
}

class Query {
  var timeouts: Map[QueryClass, QueryTimeout] = Map(
    QueryClass.Select -> QueryTimeout(5.seconds),
    QueryClass.Execute -> QueryTimeout(5.seconds)
  )

  var retries: Int = 0
  var debug: (String => Unit) = NoDebugOutput

  def apply(statsCollector: StatsCollector): QueryFactory = apply(statsCollector, None)

  def apply(statsCollector: StatsCollector, statsFactory: QueryFactory => QueryFactory): QueryFactory = apply(statsCollector, Some(statsFactory))

  def apply(statsCollector: StatsCollector, statsFactory: Option[QueryFactory => QueryFactory]): QueryFactory = {
    var queryFactory: QueryFactory = new SqlQueryFactory

    if (!timeouts.isEmpty) {
      val tupleTimeout = Map(timeouts.map { case (queryClass, timeout) =>
        (queryClass, (timeout.timeout, timeout.cancelOnTimeout))
      }.toList: _*)

      queryFactory = new PerQueryTimingOutQueryFactory(new SqlQueryFactory, tupleTimeout)
    }

    statsFactory.foreach { f =>
      queryFactory = f(queryFactory)
    }

    if (statsCollector ne NullStatsCollector) {
      queryFactory = new StatsCollectingQueryFactory(queryFactory, statsCollector)
    }

    if (retries > 0) {
      queryFactory = new RetryingQueryFactory(queryFactory, retries)
    }

    if (debug ne NoDebugOutput) {
      queryFactory = new DebuggingQueryFactory(queryFactory, debug)
    }

    queryFactory
  }

  def apply(): QueryFactory = apply(NullStatsCollector)
}
