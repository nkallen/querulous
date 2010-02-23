package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}

class StatsCollectingQueryFactory(queryFactory: QueryFactory, stats: StatsCollector)
  extends QueryFactory {

  def apply(connection: Connection, query: String, params: Any*) = {
    new StatsCollectingQuery(queryFactory(connection, query, params: _*), stats)
  }
}

class StatsCollectingQuery(query: Query, stats: StatsCollector) extends QueryProxy(query) {
  override def select[A](f: ResultSet => A) = {
    stats.incr("db-select-count", 1)
    delegate(query.select(f))
  }

  override def execute() = {
    stats.incr("db-execute-count", 1)
    delegate(query.execute())
  }

  override def delegate[A](f: => A) = {
    stats.time("db-timing")(f)
  }
}
