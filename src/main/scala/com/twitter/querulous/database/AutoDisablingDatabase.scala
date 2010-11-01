package com.twitter.querulous.database

import com.twitter.querulous.AutoDisabler
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import java.sql.{Connection, SQLException, SQLIntegrityConstraintViolationException}


class AutoDisablingDatabaseFactory(databaseFactory: DatabaseFactory, disableErrorCount: Int, disableDuration: Duration) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    new AutoDisablingDatabase(
      databaseFactory(dbhosts, dbname, username, password, urlOptions),
      dbhosts.first,
      disableErrorCount,
      disableDuration)
  }
}

class AutoDisablingDatabase(database: Database, dbhost: String, protected val disableErrorCount: Int, protected val disableDuration: Duration) extends Database with AutoDisabler {
  def open() = {
    throwIfDisabled(dbhost)
    try {
      val rv = database.open()
      noteOperationOutcome(true)
      rv
    } catch {
      case e: SQLException =>
        noteOperationOutcome(false)
        throw e
      case e: Exception =>
        throw e
    }
  }

  def close(connection: Connection) { database.close(connection) }
}
