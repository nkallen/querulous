package com.twitter.querulous.async

import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.concurrent.atomic.AtomicInteger
import java.sql.Connection
import com.twitter.util.{Throw, Future, FuturePool, JavaTimer, TimeoutException}
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
    val result = checkoutPool {
      database.open()
    }

    // release the connection if future is cancelled
    result onCancellation {
      result foreach { database.close(_) }
    }

    // cancel future if it times out
    result.within(checkoutTimer, openTimeout) onFailure {
      case _: java.util.concurrent.TimeoutException => result.cancel()
    }
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: BlockingDatabaseWrapper => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
