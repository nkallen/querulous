package com.twitter.querulous.config

import com.twitter.util.Duration
import query._

trait QueryTimeout {
  def query: String
  def name: String
  def timeout: Duration
}

trait RetryingQuery {
  def retries: Int
}

trait TimingOutQuery {
  def timeouts: Seq[QueryTimeout] = Nil
  def defaultTimeout: Duration
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
        queryFactory, map, timeoutConfig.defaultTimeout, /*statsCollector.getOrElse(NullStatsCollector)*/ NullStatsCollector)
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
