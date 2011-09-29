package com.twitter.querulous.async

import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.concurrent.atomic.AtomicInteger
import java.sql.Connection
import com.twitter.util.{Throw, Future, Promise, FuturePool, JavaTimer, TimeoutException}
import com.twitter.querulous.DaemonThreadFactory
import com.twitter.querulous.database.{Database, DatabaseFactory}


class BlockingDatabaseWrapperFactory(
  workPool: => FuturePool,
  checkoutPool: => FuturePool,
  factory: DatabaseFactory)
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
      factory(hosts, name, username, password, urlOptions, driverName)
    )
  }
}

private object AsyncConnectionCheckout {
  lazy val checkoutTimer = new JavaTimer(true)
}

class BlockingDatabaseWrapper(
  workPool: FuturePool,
  checkoutPool: FuturePool,
  protected[async] val database: Database)
extends AsyncDatabase {

  import AsyncConnectionCheckout._

  private val openTimeout  = database.openTimeout

  def withConnection[R](f: Connection => R) = {
    checkoutConnection() flatMap { conn =>
      workPool {
        try {
          f(conn)
        } finally {
          database.close(conn)
        }
      }
    }
  }

  private def checkoutConnection(): Future[Connection] = {
    // as of 11/29/11, FuturePool can throw underlying executor exceptions.
    // the try/catch can be removed when this is fixed
    try {
      val promise = new Promise[Connection]

      val checkoutResult = checkoutPool(database.open()) respond { rv =>
        // if the promise has already been set, we've timed out and we
        // need to return the connection to the pool
        if (!promise.updateIfEmpty(rv)) rv foreach database.close
      }

      // if the executor failed to submit, or we already have a connection
      // don't bother with the timer
      if (!checkoutResult.isDefined) {
        checkoutTimer.schedule(openTimeout.fromNow) {
          promise.updateIfEmpty(Throw(new TimeoutException(openTimeout.toString)))
        }
      }

      promise
    } catch {
      case t => Future.exception(t)
    }
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: BlockingDatabaseWrapper => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
