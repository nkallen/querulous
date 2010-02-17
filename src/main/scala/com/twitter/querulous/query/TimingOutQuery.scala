package com.twitter.querulous.query

import java.sql.SQLException
import com.twitter.xrayspecs.Duration


class SqlTimeoutException extends SQLException("Query timeout")

class TimingOutQueryFactory(queryFactory: QueryFactory, timeout: Duration) extends QueryFactory {
  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), timeout)
  }
}

class PerQueryTimingOutQueryFactory(queryFactory: QueryFactory, timeouts: Map[String, Duration])
  extends QueryFactory {

  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), timeouts(query))
  }
}

class TimingOutQuery(query: Query, timeout: Duration) extends QueryProxy(query) {
  override def delegate[A](f: => A) = {
    try {
      Timeout(timeout) {
        f
      } {
        cancel()
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlTimeoutException
    }
  }
}
