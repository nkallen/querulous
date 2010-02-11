package com.twitter.querulous

import java.sql.{Connection => JConnection, PreparedStatement}

class Connection(connection: JConnection, val dbhosts: Seq[String]) {
  def commit(): Unit = connection.commit()

  def close(): Unit = connection.close()

  def rollback(): Unit = connection.rollback()

  def prepareStatement(query: String): PreparedStatement = connection.prepareStatement(query)

  def setAutoCommit(value: Boolean): Unit = connection.setAutoCommit(value)
}
