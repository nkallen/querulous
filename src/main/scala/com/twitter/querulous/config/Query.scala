package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import query._

trait RetryingQuery {
  def retries: Int
}

trait TimingOutQuery {
  def timeouts: Map[String, (String, Duration, Boolean)] = Map()
  def defaultTimeout: Duration
  def cancelTimeout: Duration = 0.seconds
}

trait Query {
  def timeout: Option[TimingOutQuery]
  def retry: Option[RetryingQuery] = None
  def debug: Boolean = false

  def apply() = {
    var queryFactory: QueryFactory = new SqlQueryFactory
    timeout.foreach { timeoutConfig =>
      queryFactory = new TimingOutStatsCollectingQueryFactory(
        queryFactory, timeoutConfig.timeouts, timeoutConfig.defaultTimeout, /*statsCollector.getOrElse(NullStatsCollector)*/ NullStatsCollector)
    }

    retry.foreach { retryConfig =>
      queryFactory = new RetryingQueryFactory(queryFactory, retryConfig.retries)
    }

    if (debug) {
//      val log = Logger.get(getClass.getName)
//      queryFactory = new DebuggingQueryFactory(queryFactory, { s => log.debug(s) })
    }
    queryFactory
  }
}
