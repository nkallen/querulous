package com.twitter.querulous.query

import com.twitter.querulous.StatsCollector
import java.sql.Connection

class StatsCollectingQueryFactory(queryFactory: QueryFactory, stats: StatsCollector)
  extends QueryFactory {

  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    new StatsCollectingQuery(queryFactory(connection, queryClass, query, params: _*), queryClass, stats)
  }
}

class StatsCollectingQuery(query: Query, queryClass: QueryClass, stats: StatsCollector) extends QueryProxy(query) {
  override def delegate[A](f: => A) = {
    stats.incr("db-" + queryClass.name + "-count", 1)
    stats.time("db-" + queryClass.name + "-timing") {
      stats.time("db-timing") {
        try {
          f
        } catch {
          case e: SqlQueryTimeoutException =>
            stats.incr("db-query-timeout-count", 1)
            stats.incr("db-query-" + queryClass.name + "-timeout-count", 1)
            throw e
        }
      }
    }
  }
}
