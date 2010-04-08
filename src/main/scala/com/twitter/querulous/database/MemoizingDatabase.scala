package com.twitter.querulous.database

import scala.collection.mutable

class MemoizingDatabaseFactory(databaseFactory: DatabaseFactory) extends DatabaseFactory {
  private val databases = new mutable.HashMap[String, Database] with mutable.SynchronizedMap[String, Database]

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = synchronized {
    databases.getOrElseUpdate(
      dbhosts.first + "/" + dbname,
      databaseFactory(dbhosts, dbname, username, password))
  }

  def apply(dbhosts: List[String], username: String, password: String) = databaseFactory(dbhosts, username, password)
}
