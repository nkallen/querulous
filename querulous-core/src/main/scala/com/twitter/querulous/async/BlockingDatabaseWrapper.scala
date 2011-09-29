package com.twitter.querulous.async

import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.concurrent.atomic.AtomicInteger
import java.sql.Connection
import com.twitter.util.{Throw, Future, Promise, FuturePool, JavaTimer, TimeoutException}
import com.twitter.querulous.DaemonThreadFactory
import com.twitter.querulous.database.{Database, DatabaseFactory}


class BlockingDatabaseWrapperFactory(
  pool: => FuturePool,
  factory: DatabaseFactory,
  maxWaiters: Int)
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
      pool,
      factory(hosts, name, username, password, urlOptions, driverName),
      maxWaiters
    )
  }
}

private object AsyncConnectionCheckout {
  lazy val checkoutTimer = new JavaTimer(true)
}

class BlockingDatabaseWrapper(
  pool: FuturePool,
  protected[async] val database: Database,
  maxWaiters: Int)
extends AsyncDatabase {

  import AsyncConnectionCheckout._

  // XXX: this probably should be configurable as well.
  private val checkoutPool = FuturePool(Executors.newSingleThreadExecutor(new DaemonThreadFactory))
  private val openTimeout  = database.openTimeout
  private val waiters = new AtomicInteger(0)

  def withConnection[R](f: Connection => R) = {
    checkoutConnection() flatMap { conn =>
      pool {
        try {
          f(conn)
        } finally {
          database.close(conn)
        }
      }
    }
  }

  private def checkoutConnection(): Future[Connection] = {
    if (waiters.get > maxWaiters) {
      Future.exception(new RejectedExecutionException("too many waiters"))
    } else {
      val promise = new Promise[Connection]
      waiters.incrementAndGet

      checkoutPool(database.open()) respond { rv =>
        if (promise.updateIfEmpty(rv))
          // decrement only if update succeeds
          waiters.decrementAndGet
        else
          // if the promise has already timed out, we need to close the connection here.
          rv foreach database.close
      }

      checkoutTimer.schedule(openTimeout.fromNow) {
        if (promise.updateIfEmpty(Throw(new TimeoutException(openTimeout.toString))))
          // decrement only if update succeeds
          waiters.decrementAndGet
      }

      promise
    }
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: BlockingDatabaseWrapper => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
