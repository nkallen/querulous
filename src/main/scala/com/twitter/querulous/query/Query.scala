package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}
import scala.collection.mutable
import com.twitter.querulous.StatsCollector
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


trait QueryFactory {
  def apply(connection: Connection, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def addParams(params: Any*)
  def cancel()
}

object QueryFactory {
  private def convertConfigMap(queryMap: ConfigMap) = {
    val queryInfo = new mutable.HashMap[String, (String, Duration)]
    for (key <- queryMap.keys) {
      val pair = queryMap.getList(key)
      val query = pair(0)
      val timeout = pair(1).toLong.millis
      queryInfo += (query -> (key, timeout))
    }
    queryInfo
  }

  /*
    query_timeout_default = 3000
    queries {
      select_source_id_for_update = ["SELECT * FROM ? WHERE source_id = ? FOR UPDATE", 3000]
    }
    retries = 3
    debug = false
  */

  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]): QueryFactory = {
    var queryFactory: QueryFactory = new SqlQueryFactory
    config.getConfigMap("queries") match {
      case Some(queryMap) =>
        val queryInfo = convertConfigMap(queryMap)
        val timeout = config("query_timeout_default").toLong.millis
        queryFactory = new TimingOutStatsCollectingQueryFactory(queryFactory, queryInfo, timeout,
                                                                statsCollector.get)
      case None =>
        config.getInt("query_timeout_default").foreach { timeout =>
          queryFactory = new TimingOutQueryFactory(queryFactory, timeout.millis)
        }
        statsCollector.foreach { stats =>
          queryFactory = new StatsCollectingQueryFactory(queryFactory, stats)
        }
    }

    config.getInt("retries").foreach { retries =>
      queryFactory = new RetryingQueryFactory(queryFactory, retries)
    }
    if (config.getBool("debug", false)) {
      val log = Logger.get(getClass.getName)
      queryFactory = new DebuggingQueryFactory(queryFactory, { s => log.debug(s) })
    }
    queryFactory
  }

  def fromConfig(config: querulous.config.Query, statsCollector: Option[StatsCollector]) = {
    var queryFactory: QueryFactory = new SqlQueryFactory
    config.timeout.foreach { timeoutConfig =>
      val map = Map[String, (String, Duration)](timeoutConfig.timeouts.map { timeout =>
        (timeout.query -> (timeout.name, timeout.timeout))
      }.toArray: _*)

      queryFactory = new TimingOutStatsCollectingQueryFactory(
        queryFactory, map, timeoutConfig.defaultTimeout, statsCollector.getOrElse(NullStatsCollector))
    }

    config.retry.foreach { retryConfig =>
      queryFactory = new RetryingQueryFactory(queryFactory, retryConfig.retries)
    }

    if (config.debug) {
      val log = Logger.get(getClass.getName)
      queryFactory = new DebuggingQueryFactory(queryFactory, { s => log.debug(s) })
    }
    queryFactory
  }
}
