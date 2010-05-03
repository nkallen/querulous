package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}
import scala.collection.mutable
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


trait QueryFactory {
  def apply(connection: Connection, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
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
}