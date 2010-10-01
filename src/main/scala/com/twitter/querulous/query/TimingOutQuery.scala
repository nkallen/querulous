package com.twitter.querulous.query

import java.sql.{SQLException, Connection}
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._


class SqlQueryTimeoutException(val timeout: Duration) extends SQLException("Query timeout: " + timeout.inMillis + " msec")

/**
 * A {@code QueryFactory} that creates {@link Query}s that execute subject to a {@code timeout}.  An
 * attempt to {@link Query#cancel} the query is made if the timeout expires.
 *
 * <p>Note that queries timing out promptly is based upon {@link java.sql.Statement#cancel} working
 * and executing promptly for the JDBC driver in use.
 */
class TimingOutQueryFactory(queryFactory: QueryFactory, timeout: Duration, cancelTimeout: Duration) extends QueryFactory {
  def this(queryFactory: QueryFactory, timeout: Duration) = this(queryFactory, timeout, 0.millis)

  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), connection, timeout, cancelTimeout)
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
class PerQueryTimingOutQueryFactory(queryFactory: QueryFactory, timeouts: Map[String, Duration], cancelTimeout: Duration)
  extends QueryFactory {

  def this(queryFactory: QueryFactory, timeouts: Map[String, Duration]) = this(queryFactory, timeouts, 0.millis)

  def apply(connection: Connection, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, query, params: _*), connection, timeouts(query), cancelTimeout)
  }
}

private object QueryCancellation {
  val cancelTimer = new java.util.Timer("global query cancellation timer", true)
}

/**
 * A {@code Query} that executes subject to the {@code timeout} specified.  An attempt to
 * {@link #cancel} the query is made if the timeout expires before the query completes.
 *
 * <p>Note that the query timing out promptly is based upon {@link java.sql.Statement#cancel}
 * working and executing promptly for the JDBC driver in use.
 */
class TimingOutQuery(query: Query, connection: Connection, timeout: Duration, cancelTimeout: Duration)
  extends QueryProxy(query) with ConnectionDestroying {

  import QueryCancellation._

  override def delegate[A](f: => A) = {
    try {
      // outer timeout clobbers the connection if the inner cancel fails to unblock the connection
      Timeout(cancelTimer, timeout + cancelTimeout) {
        Timeout(cancelTimer, timeout)(f)(cancel)
      } {
        destroyConnection(connection)
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlQueryTimeoutException(timeout)
    }
  }

  override def cancel() {
    val cancelThread = new Thread("query cancellation") {
      override def run() {
        try {
          // This cancel may block, as it has to connect to the database.
          // If the default socket connection timeout has been removed, this thread will run away.
          query.cancel()
        } catch { case e => () }
      }
    }
    cancelThread.start()
  }
}
