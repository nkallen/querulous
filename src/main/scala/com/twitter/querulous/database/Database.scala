package com.twitter.querulous.database

trait DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database
}

trait Database {
  def reserve(): Connection

  def release(connection: Connection)

  protected def url(dbhosts: List[String], dbname: String) = {
    val dbnameSegment = if (dbname == null) "" else ("/" + dbname)
    "jdbc:mysql://" + dbhosts.mkString(",") + dbnameSegment + "?useUnicode=true&characterEncoding=UTF-8"
  }

  def close()
}
