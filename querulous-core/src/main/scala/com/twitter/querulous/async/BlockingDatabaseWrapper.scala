package com.twitter.querulous.async

import java.util.logging.{Logger, Level}
import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.sql.Connection
import com.twitter.util.{Throw, Future, Promise}
import com.twitter.util.{FuturePool, ExecutorServiceFuturePool, JavaTimer, TimeoutException}
import com.twitter.querulous.{StatsCollector, NullStatsCollector, DaemonThreadFactory}
import com.twitter.querulous.database.{Database, DatabaseFactory}


class BlockingDatabaseWrapperFactory(
  workPool: => FuturePool,
  checkoutPool: => FuturePool,
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
      checkoutPool,
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
  checkoutPool: FuturePool,
  protected[async] val database: Database,
  stats: StatsCollector = NullStatsCollector)
extends AsyncDatabase {

  import AsyncConnectionCheckout._

  getExecutor(workPool) foreach { e =>
    stats.addGauge("db-async-active-threads")(e.getActiveCount.toDouble)
  }

  getExecutor(checkoutPool) foreach { e =>
    val q = e.getQueue
    stats.addGauge("db-async-waiters")(q.size.toDouble)
  }

  private val openTimeout  = database.openTimeout

  def withConnection[R](f: Connection => R) = {
    checkoutConnection() flatMap { conn =>

      // TODO: remove the atomic boolean and the close attempt in the
      // try/finally block.
      //
      // As a workaround for a FuturePool bug where it may throw away
      // work if cancelled but already in progress, therefore not
      // allowing ensure to predictably clean up, use an AtomicBoolean
      // to allow ensure and the work block to race to steal the
      // connection.
      val inProgress = new AtomicBoolean(false)

      workPool {
        if(inProgress.compareAndSet(false, true)) {
          try {
            f(conn)
          } finally {
            database.close(conn)
          }
        } else {
          // not truly an error in this case, but we need something
          // that evalutates to Nothing here.
          error("Lost race with ensure block. Connection closed.")
        }
      } ensure {
        if (inProgress.compareAndSet(false, true)) {
          database.close(conn)
        }
      }
    }
  }

  private def checkoutConnection(): Future[Connection] = {
    // TODO: remove the explicit promise creation once twitter util
    // gets updated.
    //
    // creating a detached promise here so that cancellations do not
    // propagate to the checkout pool.
    val result = new Promise[Connection]

    checkoutPool { database.open() } respond { result.update(_) } onFailure { e =>
      if (e.isInstanceOf[java.util.concurrent.RejectedExecutionException]) {
        stats.incr("db-async-open-rejected-count", 1)
      }
    }

    // Return within a specified timeout. If within times out, that
    // means the connection is never handed off, so we need to clean
    // up ourselves.
    result.within(checkoutTimer, openTimeout) onFailure { e =>
      if (e.isInstanceOf[java.util.concurrent.TimeoutException]) {

        stats.incr("db-async-open-timeout-count", 1)

        // Cancel the checkout.
        result.cancel()

        // If the future is not cancelled in time, close the dangling
        // connection.
        result foreach { c =>
          try {
            database.close(c)
          } catch {
            case e => Logger.getLogger("querulous").log(
              Level.WARNING,
              "Exception on database close.",
              e
            )
          }
        }
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
