package com.twitter.querulous.query

import java.sql.{SQLException, Connection}
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import scala.collection.Map
import com.twitter.querulous.{Timeout, TimeoutException}


class SqlQueryTimeoutException(val timeout: Duration) extends SQLException("Query timeout: " + timeout.inMillis + " msec")

/**
 * A {@code QueryFactory} that creates {@link Query}s that execute subject to a {@code timeout}.  An
 * attempt to {@link Query#cancel} the query is made if the timeout expires.
 *
 * <p>Note that queries timing out promptly is based upon {@link java.sql.Statement#cancel} working
 * and executing promptly for the JDBC driver in use.
 */
class TimingOutQueryFactory(queryFactory: QueryFactory, val timeout: Duration, val cancelOnTimeout: Boolean)
  extends QueryFactory {

  def this(queryFactory: QueryFactory, timeout: Duration) = this(queryFactory, timeout, false)

  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    new TimingOutQuery(queryFactory(connection, queryClass, query, params: _*), connection, timeout, cancelOnTimeout)
  }
}

/**
 * A `QueryFactory` that creates `Query`s that execute subject to the timeouts
 * specified for individual query classes.
 */
class PerQueryTimingOutQueryFactory(queryFactory: QueryFactory, val timeouts: Map[QueryClass, (Duration, Boolean)])
  extends QueryFactory {

  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    val (timeout, cancelOnTimeout) = timeouts(queryClass)
    new TimingOutQuery(queryFactory(connection, queryClass, query, params: _*), connection, timeout, cancelOnTimeout)
  }
}

private object QueryCancellation {
  lazy val cancelTimer = new java.util.Timer("global query cancellation timer", true)
}

/**
 * A {@code Query} that executes subject to the {@code timeout} specified.  An attempt to
 * {@link #cancel} the query is made if the timeout expires before the query completes.
 *
 * <p>Note that the query timing out promptly is based upon {@link java.sql.Statement#cancel}
 * working and executing promptly for the JDBC driver in use.
 */
class TimingOutQuery(query: Query, connection: Connection, timeout: Duration, cancelOnTimeout: Boolean)
  extends QueryProxy(query) with ConnectionDestroying {

  def this(query: Query, connection: Connection, timeout: Duration) = this(query, connection, timeout, false)

  import QueryCancellation._

  override def delegate[A](f: => A) = {
    try {
      Timeout(cancelTimer, timeout)(f) {
        if (cancelOnTimeout) cancel()
        destroyConnection(connection)
      }
    } catch {
      case e: TimeoutException => throw new SqlQueryTimeoutException(timeout)
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
