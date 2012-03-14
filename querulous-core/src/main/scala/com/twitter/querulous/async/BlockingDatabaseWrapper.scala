package com.twitter.querulous.async

import java.util.logging.{Logger, Level}
import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.sql.Connection
import com.twitter.util.{Try, Throw, Future, Promise}
import com.twitter.util.{FuturePool, ExecutorServiceFuturePool, JavaTimer, TimeoutException}
import com.twitter.querulous.{StatsCollector, NullStatsCollector, DaemonThreadFactory}
import com.twitter.querulous.database.{Database, DatabaseFactory}


class BlockingDatabaseWrapperFactory(
  workPool: => FuturePool,
  factory: DatabaseFactory,
  stats: StatsCollector = NullStatsCollector)
extends AsyncDatabaseFactory {
  def apply(
    hosts: List[String],
    name: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driverName: String
  ): AsyncDatabase = {
    new BlockingDatabaseWrapper(
      workPool,
      factory(hosts, name, username, password, urlOptions, driverName),
      stats
    )
  }
}

private object AsyncConnectionCheckout {
  lazy val checkoutTimer = new JavaTimer(true)
}

class BlockingDatabaseWrapper(
  workPool: FuturePool,
  protected[async] val database: Database,
  stats: StatsCollector = NullStatsCollector)
extends AsyncDatabase {

  import AsyncConnectionCheckout._

  getExecutor(workPool) foreach { e =>
    stats.addGauge("db-async-active-threads")(e.getActiveCount.toDouble)
  }

  private val openTimeout  = database.openTimeout

  def withConnection[R](f: Connection => R) = {
    checkoutConnection() flatMap { conn =>
      workPool {
        f(conn)
      } ensure {
        closeConnection(conn)
      }
    }
  }

  private def checkoutConnection(): Future[Connection] = {

    // XXX: there is a bug in FuturePool before util 1.12.5 that
    // causes it to potentially drop completed work when cancelled. As
    // a workaround, we create a promise explicitly and set it from
    // the future pool in order to prevent the checkout FuturePool
    // from receiving cancellation signals.
    val conn = new Promise[Connection]()

    stats.timeFutureMillis("db-async-open-timing") {
      workPool { conn() = Try(database.open()) }
    }

    // Return within a specified timeout. If within times out, that
    // means the connection is never handed off, so we need to clean
    // up ourselves.
    conn.within(checkoutTimer, openTimeout) onFailure {
      case e: java.util.concurrent.RejectedExecutionException => {
        stats.incr("db-async-open-rejected-count", 1)
      }
      case e: java.util.concurrent.TimeoutException => {
        stats.incr("db-async-open-timeout-count", 1)

        // Cancel the checkout.
        conn.cancel()

        // If the future is not cancelled in time, this will close the
        // dangling connection.
        conn foreach { closeConnection(_) }
      }
      case _ => {}
    }
  }

  private def closeConnection(c: Connection) {
    try {
      database.close(c)
    } catch {
      case e => {
        val l = Logger.getLogger("querulous")
        l.log(Level.WARNING, "Exception on database close.", e)
      }
    }
  }

  private def getExecutor(p: FuturePool) = p match {
    case p: ExecutorServiceFuturePool => p.executor match {
      case e: java.util.concurrent.ThreadPoolExecutor => Some(e)
      case _ => None
    }
    case _ => None
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: BlockingDatabaseWrapper => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
