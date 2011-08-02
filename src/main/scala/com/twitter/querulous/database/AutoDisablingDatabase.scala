package com.twitter.querulous.database

import com.twitter.querulous.AutoDisabler
import com.twitter.util.Duration
import java.sql.{Connection, SQLException}


class AutoDisablingDatabaseFactory(val databaseFactory: DatabaseFactory, val disableErrorCount: Int, val disableDuration: Duration) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    new AutoDisablingDatabase(
      databaseFactory(dbhosts, dbname, username, password, urlOptions),
      disableErrorCount,
      disableDuration)
  }
}

class AutoDisablingDatabase(
  val database: Database,
  protected val disableErrorCount: Int,
  protected val disableDuration: Duration)
extends Database
with DatabaseProxy
with AutoDisabler {
  def open() = {
    throwIfDisabled(database.hosts.head)
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
