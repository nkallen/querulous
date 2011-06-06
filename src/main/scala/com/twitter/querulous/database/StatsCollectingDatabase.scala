package com.twitter.querulous.database

import com.twitter.querulous.StatsCollector
import java.sql.Connection

class StatsCollectingDatabaseFactory(
  databaseFactory: DatabaseFactory,
  stats: StatsCollector) extends DatabaseFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    new StatsCollectingDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions), stats)
  }
}

class StatsCollectingDatabase(val database: Database, stats: StatsCollector)
extends Database
with DatabaseProxy {

  override def open(): Connection = {
    stats.time("db-open-timing") {
      try {
        database.open()
      } catch {
        case e: SqlDatabaseTimeoutException =>
          stats.incr("db-open-timeout-count", 1)
          throw e
      }
    }
  }

  override def close(connection: Connection) = {
    stats.time("db-close-timing") {
      try {
        database.close(connection)
      } catch {
        case e: SqlDatabaseTimeoutException =>
          stats.incr("db-close-timeout-count", 1)
          throw e
      }
    }
  }
}
