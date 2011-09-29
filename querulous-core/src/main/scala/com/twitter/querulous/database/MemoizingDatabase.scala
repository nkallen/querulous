package com.twitter.querulous.database

import scala.collection.mutable

class MemoizingDatabaseFactory(val databaseFactory: DatabaseFactory) extends DatabaseFactory {
  private val databases = new mutable.HashMap[String, Database] with mutable.SynchronizedMap[String, Database]

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driverName: String) = synchronized {
    databases.getOrElseUpdate(
      dbhosts.toString + "/" + dbname,
      databaseFactory(dbhosts, dbname, username, password, urlOptions, driverName))
  }

  // cannot memoize a connection without specifying a database
  override def apply(dbhosts: List[String], username: String, password: String) = databaseFactory(dbhosts, username, password)
}
