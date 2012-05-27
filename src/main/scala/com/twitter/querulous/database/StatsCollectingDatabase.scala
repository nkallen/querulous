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

class StatsCollectingDatabase(database: Database, stats: StatsCollector)
  extends Database {

  override def open(): Connection = {
    stats.time("database-open-timing") {
      database.open()
    }
  }

  override def close(connection: Connection) = {
    stats.time("database-close-timing") {
      database.close(connection)
    }
  }
}
