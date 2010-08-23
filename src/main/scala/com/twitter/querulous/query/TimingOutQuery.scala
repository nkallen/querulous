package com.twitter.querulous.query

import java.sql.{SQLException, Connection}
import com.twitter.xrayspecs.Duration


class SqlQueryTimeoutException extends SQLException("Query timeout")

/**
 * A {@code QueryFactory} that creates {@link Query}s that execute subject to a {@code timeout}.  An
 * attempt to {@link Query#cancel} the query is made if the timeout expires.
 * 
 * <p>Note that queries timing out promptly is based upon {@link java.sql.Statement#cancel} working
 * and executing promptly for the JDBC driver in use.
 */
class TimingOutQueryFactory(queryFactory: QueryFactory, timeout: Duration) extends QueryFactory {
  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), timeout)
  }
}

/**
 * A {@code QueryFactory} that creates {@link Query}s that execute subject to the {@code timeouts}
 * specified for individual queries.  An attempt to {@link Query#cancel} a query is made if the
 * timeout expires.
 * 
 * <p>Note that queries timing out promptly is based upon {@link java.sql.Statement#cancel} working
 * and executing promptly for the JDBC driver in use.
 */
class PerQueryTimingOutQueryFactory(queryFactory: QueryFactory, timeouts: Map[String, Duration])
  extends QueryFactory {

  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), timeouts(query))
  }
}

/**
 * A {@code Query} that executes subject to the {@code timeout} specified.  An attempt to
 * {@link #cancel} the query is made if the timeout expires before the query completes.
 *
 * <p>Note that the query timing out promptly is based upon {@link java.sql.Statement#cancel}
 * working and executing promptly for the JDBC driver in use.
 */
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
        throw new SqlQueryTimeoutException
    }
  }
}
