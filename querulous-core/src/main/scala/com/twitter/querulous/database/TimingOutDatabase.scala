package com.twitter.querulous.database

import com.twitter.querulous.{FutureTimeout, TimeoutException}
import java.sql.{Connection, SQLException}
import com.twitter.util.Duration


class SqlDatabaseTimeoutException(msg: String, val timeout: Duration) extends SQLException(msg)

class TimingOutDatabaseFactory(
  val databaseFactory: DatabaseFactory,
  val poolSize: Int,
  val queueSize: Int,
  val openTimeout: Duration)
extends DatabaseFactory {

  private def newTimeoutPool() = new FutureTimeout(poolSize, queueSize)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String,
            urlOptions: Map[String, String], driverName: String) = {

    new TimingOutDatabase(
      databaseFactory(dbhosts, dbname, username, password, urlOptions, driverName),
      newTimeoutPool(),
      openTimeout
    )
  }
}

class TimingOutDatabase(
  val database: Database,
  timeout: FutureTimeout,
  openTimeout: Duration)
extends Database
with DatabaseProxy {
  val label = database.name match {
    case null => database.hosts.mkString(",") +"/ (null)"
    case name => database.hosts.mkString(",") +"/"+ name
  }

  private def getConnection(wait: Duration) = {
    try {
      timeout(wait) {
        database.open()
      } { conn =>
        database.close(conn)
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlDatabaseTimeoutException(label, wait)
    }
  }

  override def open() = getConnection(openTimeout)

  def close(connection: Connection) { database.close(connection) }
}
