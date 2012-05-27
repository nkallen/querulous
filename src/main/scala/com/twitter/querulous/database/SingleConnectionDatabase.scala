package com.twitter.querulous.database

import org.apache.commons.dbcp.DriverManagerConnectionFactory
import java.sql.{SQLException, Connection}


class SingleConnectionDatabaseFactory(defaultUrlOptions: Map[String, String]) extends DatabaseFactory {
  def this() = this(Map.empty)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    val finalUrlOptions =
      if (urlOptions eq null) {
        defaultUrlOptions
      } else {
        defaultUrlOptions ++ urlOptions
      }

    new SingleConnectionDatabase(dbhosts, dbname, username, password, finalUrlOptions)
  }
}

class SingleConnectionDatabase(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String])
  extends Database {
  Class.forName("com.mysql.jdbc.Driver")
  private val connectionFactory = new DriverManagerConnectionFactory(url(dbhosts, dbname, urlOptions), username, password)

  def close(connection: Connection) {
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }
  }

  def open() = connectionFactory.createConnection()
  override def toString = dbhosts.head + "_" + dbname
}
