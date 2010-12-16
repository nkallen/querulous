package com.twitter.querulous.database

import com.twitter.querulous.{FutureTimeout, TimeoutException}
import java.sql.{Connection, SQLException}
import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.util.Duration
import net.lag.logging.Logger


class SqlDatabaseTimeoutException(msg: String, val timeout: Duration) extends SQLException(msg)

class TimingOutDatabaseFactory(val databaseFactory: DatabaseFactory,
                               val poolSize: Int,
                               val queueSize: Int,
                               val openTimeout: Duration,
                               val maxConnections: Int) extends DatabaseFactory {
  private val timeout = new FutureTimeout(poolSize, queueSize)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String,
            urlOptions: Map[String, String]) = {
    val dbLabel = if (dbname != null) dbname else "(null)"

    new TimingOutDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions),
                          dbhosts, dbLabel, timeout, openTimeout, maxConnections)
  }
}

class TimingOutDatabase(database: Database, dbhosts: List[String], dbname: String,
                        timeout: FutureTimeout, openTimeout: Duration,
                        maxConnections: Int) extends Database {
  private val log = Logger.get(getClass.getName)

  private def getConnection(wait: Duration) = {
    try {
      timeout(wait) {
        database.open()
      } { conn =>
        database.close(conn)
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlDatabaseTimeoutException(dbhosts.mkString(",") + "/" + dbname, wait)
    }
  }

  override def open() = getConnection(openTimeout)

  def close(connection: Connection) { database.close(connection) }
}
