package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import query._

class QueryTimeout(val query: String, val name: String, val timeout: Duration)

trait RetryingQuery {
  def retries: Int
}

trait TimingOutQuery {
  def timeouts: Seq[QueryTimeout] = Nil
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
      val map = Map[String, (String, Duration)](timeoutConfig.timeouts.map { timeout =>
        (timeout.query -> (timeout.name, timeout.timeout))
      }.toArray: _*)

      queryFactory = new TimingOutStatsCollectingQueryFactory(
        queryFactory, map, timeoutConfig.defaultTimeout, timeoutConfig.cancelTimeout, /*statsCollector.getOrElse(NullStatsCollector)*/ NullStatsCollector)
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
