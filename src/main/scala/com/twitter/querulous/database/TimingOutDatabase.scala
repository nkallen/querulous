package com.twitter.querulous.database

import java.sql.{Connection, SQLException}
import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration
import net.lag.logging.Logger


class SqlDatabaseTimeoutException(msg: String) extends SQLException(msg)

class TimingOutDatabaseFactory(databaseFactory: DatabaseFactory, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration, maxConnections: Int) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: String) = {
    new TimingOutDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions), dbhosts, dbname, poolSize, queueSize, openTimeout, initialTimeout, maxConnections)
  }

  override def apply(dbhosts: List[String], username: String, password: String) = {
    new TimingOutDatabase(databaseFactory(dbhosts, username, password), dbhosts, "(null)", poolSize, queueSize, openTimeout, initialTimeout, maxConnections)
  }
}

class TimingOutDatabase(database: Database, dbhosts: List[String], dbname: String, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration, maxConnections: Int) extends Database {
  private val timeout = new FutureTimeout(poolSize, queueSize)
  private val log = Logger.get(getClass.getName)

  // FIXME not working yet.
  //greedilyInstantiateConnections()

  private def getConnection(wait: Duration) = {
    try {
      timeout(wait) {
        database.open()
      } { conn =>
        database.close(conn)
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlDatabaseTimeoutException(dbhosts.mkString(",") + "/" + dbname)
    }
  }

  private def greedilyInstantiateConnections() = {
    log.info("Connecting to %s:%s", dbhosts.mkString(","), dbname)
    (0 until maxConnections).force.map { i =>
      getConnection(initialTimeout)
    }.map(_.close)
  }

  override def open() = getConnection(openTimeout)

  def close(connection: Connection) { database.close(connection) }
}
