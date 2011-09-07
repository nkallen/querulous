package com.twitter.querulous.database

import com.twitter.querulous.StatsCollector
import java.sql.Connection

class StatsCollectingDatabaseFactory(
  databaseFactory: DatabaseFactory,
  name: Option[String],
  stats: StatsCollector) extends DatabaseFactory {

  def this(databaseFactory: DatabaseFactory, stats: StatsCollector) = this(databaseFactory, None, stats)
  def this(databaseFactory: DatabaseFactory, name: String, stats: StatsCollector) = this(databaseFactory, Some(name), stats)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driverName: String) = {
    new StatsCollectingDatabase(databaseFactory(dbhosts, dbname, username, password, urlOptions, driverName), name, stats)
  }
}

class StatsCollectingDatabase(val database: Database, name: Option[String], stats: StatsCollector)
extends Database
with DatabaseProxy {

  val innerDbGauges = database match {
    case dbProxy: DatabaseProxy => dbProxy.getInnermostDatabase().getGauges
    case _ => database.getGauges
  }

  innerDbGauges foreach { x =>
    x match {
      case (name, gauge) => stats.addGauge(name)(gauge())
    }
  }

  def this(database: Database, stats: StatsCollector) = this(database, None, stats)
  def this(database: Database, name: String, stats: StatsCollector) = this(database, Some(name), stats)

  override def open(): Connection = {
    stats.time("db-open-timing") {
      try {
        database.open()
      } catch {
        case e: SqlDatabaseTimeoutException =>
          name.foreach { n => stats.incr("db-"+n+"-open-timeout-count", 1) }
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
          name.foreach { n => stats.incr("db-"+n+"-close-timeout-count", 1) }
          stats.incr("db-close-timeout-count", 1)
          throw e
      }
    }
  }
}
