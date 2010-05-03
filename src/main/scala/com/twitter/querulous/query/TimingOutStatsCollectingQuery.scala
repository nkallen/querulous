package com.twitter.querulous.query

import java.sql.{Connection, ResultSet}
import scala.collection.Map
import scala.util.matching.Regex
import scala.collection.Map
import com.twitter.xrayspecs.Duration
import net.lag.extensions._


object TimingOutStatsCollectingQueryFactory {
  val TABLE_NAME = """(FROM|UPDATE|INSERT INTO|LIMIT)\s+[\w-]+""".r
  val DDL_QUERY = """^\s*((CREATE|DROP|ALTER)\s+(TABLE|DATABASE)|DESCRIBE)\s+""".r

  def simplifiedQuery(query: String) = {
    if (DDL_QUERY.findFirstMatchIn(query).isDefined) {
      "default"
    } else {
      query.regexSub(TABLE_NAME) { m => m.group(1) + "?" }
    }
  }
}

class TimingOutStatsCollectingQueryFactory(queryFactory: QueryFactory,
                                           queryInfo: Map[String, (String, Duration)],
                                           defaultTimeout: Duration, stats: StatsCollector)
      extends QueryFactory {
  def apply(connection: Connection, query: String, params: Any*) = {
    val simplifiedQueryString = TimingOutStatsCollectingQueryFactory.simplifiedQuery(query)
    val (name, timeout) = queryInfo.getOrElse(simplifiedQueryString, ("default", defaultTimeout))
    new TimingOutStatsCollectingQuery(new TimingOutQuery(queryFactory(connection, query, params: _*), timeout), name, stats)
  }
}

class TimingOutStatsCollectingQuery(query: Query, queryName: String, stats: StatsCollector) extends QueryProxy(query) {
  override def select[A](f: ResultSet => A) = {
    stats.incr("db-select-count", 1)
    stats.time("db-select-timing")(delegate(query.select(f)))
  }

  override def execute() = {
    stats.incr("db-execute-count", 1)
    stats.time("db-execute-timing")(delegate(query.execute()))
  }

  override def delegate[A](f: => A) = {
    stats.incr("db-query-count-" + queryName, 1)
    stats.time("db-timing") {
      stats.time("x-db-query-timing-" + queryName) {
        f
      }
    }
  }
}
