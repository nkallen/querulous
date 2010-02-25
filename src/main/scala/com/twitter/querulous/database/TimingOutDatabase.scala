package com.twitter.querulous.database

import java.sql.{Connection, SQLException}
import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration

class SqlDatabaseTimeoutException(msg: String) extends SQLException(msg)

class TimingOutDatabaseFactory(databaseFactory: DatabaseFactory, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration, maxConnections: Int) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database = {
    new TimingOutDatabase(databaseFactory(dbhosts, dbname, username, password), dbhosts, dbname, poolSize, queueSize, openTimeout, initialTimeout, maxConnections)
  }
}

class TimingOutDatabase(database: Database, dbhosts: List[String], dbname: String, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration, maxConnections: Int) extends Database {
  private val timeout = new FutureTimeout(poolSize, queueSize)

  greedilyInstantiateConnections()

  private def getConnection(wait: Duration) = {
    var connection: Connection = null
    try {
      timeout(wait) {
        connection = database.open()
      } {
        database.close(connection)
      }
      connection
    } catch {
      case e: TimeoutException => throw new SqlDatabaseTimeoutException(dbhosts.mkString(",")+"/"+dbname)
    }
  }

  private def greedilyInstantiateConnections() = {
    (0 until maxConnections).map { i =>
      getConnection(initialTimeout)
    }.map(_.close)
  }

  override def open() = getConnection(openTimeout)

  def close(connection: Connection) { database.close(connection) }
}
