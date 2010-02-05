package com.twitter.querulous.connectionpool

import java.sql.Connection

trait ConnectionPoolFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): ConnectionPool
}

trait ConnectionPool {
  def reserve(): Connection

  def release(connection: Connection)

  protected def url(dbhosts: List[String], dbname: String) = {
    val dbnameSegment = if (dbname == null) "" else ("/" + dbname)
    "jdbc:mysql://" + dbhosts.mkString(",") + dbnameSegment + "?useUnicode=true&characterEncoding=UTF-8"
  }

  def close()
}
