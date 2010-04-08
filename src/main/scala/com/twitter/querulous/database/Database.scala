package com.twitter.querulous.database

import java.sql.Connection

trait DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database
  def apply(dbhosts: List[String], username: String, password: String): Database
}

trait Database {
  def open(): Connection

  def close(connection: Connection)

  def withConnection[A](f: Connection => A): A = {
    val connection = open()
    try {
      f(connection)
    } finally {
      close(connection)
    }
  }

  protected def url(dbhosts: List[String], dbname: String) = {
    val dbnameSegment = if (dbname == null) "" else ("/" + dbname)
    "jdbc:mysql://" + dbhosts.mkString(",") + dbnameSegment + "?useUnicode=true&characterEncoding=UTF-8"
  }
}
