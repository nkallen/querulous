package com.twitter.querulous.async

import java.util.concurrent.Executors
import java.sql.Connection
import com.twitter.util.{Throw, Future, Promise, FuturePool, JavaTimer, TimeoutException}
import com.twitter.querulous.DaemonThreadFactory
import com.twitter.querulous.database.{Database, DatabaseFactory}


class BlockingDatabaseWrapperFactory(pool: => FuturePool, factory: DatabaseFactory)
extends AsyncDatabaseFactory {
  def apply(
    hosts: List[String],
    name: String,
    username: String,
    password: String,
    urlOptions: Map[String, String]
  ): AsyncDatabase = {
    new BlockingDatabaseWrapper(
      pool,
      factory(hosts, name, username, password, urlOptions)
    )
  }
}

private object AsyncConnectionCheckout {
  lazy val checkoutTimer = new JavaTimer(true)
}

class BlockingDatabaseWrapper(
  pool: FuturePool,
  protected[async] val database: Database)
extends AsyncDatabase {

  import AsyncConnectionCheckout._

  // XXX: this probably should be configurable as well.
  private val checkoutPool = FuturePool(Executors.newSingleThreadExecutor(new DaemonThreadFactory))
  private val openTimeout  = database.openTimeout

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
    val promise = new Promise[Connection]

    checkoutPool(database.open()) respond { rv =>
      // if the promise has already timed out, we need to close the connection here.
      if (!promise.updateIfEmpty(rv)) rv foreach database.close
    }

    checkoutTimer.schedule(openTimeout.fromNow) {
      promise.updateIfEmpty(Throw(new TimeoutException(openTimeout.toString)))
    }

    promise
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: BlockingDatabaseWrapper => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
