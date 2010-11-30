package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}
import scala.collection.mutable
import com.twitter.querulous.StatsCollector
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


trait QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def addParams(params: Any*)
  def cancel()
}

object QueryFactory {
  private def convertConfigMap(queryMap: ConfigMap) = {
    val queryInfo = new mutable.HashMap[String, (String, Duration, Boolean)]
    for (key <- queryMap.keys) {
      val pair = queryMap.getList(key)
      val query = pair(0)
      val timeout = pair(1).toLong.millis
      queryInfo += (query -> (key, timeout, false))
    }
    queryInfo
  }

  /*
    timeouts {
      select = 100
      execute = 5000
    }
    cancel_on_timeout = [ "select" ]
    retries = 3
    debug = false
  */
  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]): QueryFactory = {
    var queryFactory: QueryFactory = new SqlQueryFactory
    val cancelOnTimeout = config.getList("cancel_on_timeout")
    config.getConfigMap("timeouts") match {
      case None =>
        config.getInt("query_timeout_default").foreach { timeout =>
          queryFactory = new TimingOutQueryFactory(queryFactory, timeout.millis, false)
        }
      case Some(timeoutMap) =>
        val timeouts = mutable.Map[QueryClass, (Duration, Boolean)]()
        timeoutMap.keys.foreach { queryClass =>
          val cancel = cancelOnTimeout.contains(queryClass)
          timeouts(QueryClass.lookup(queryClass)) = (timeoutMap(queryClass).toInt.milliseconds, cancel)
        }
        queryFactory = new PerQueryTimingOutQueryFactory(queryFactory, timeouts)
    }

    statsCollector.foreach { stats =>
      queryFactory = new StatsCollectingQueryFactory(queryFactory, stats)
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
}
