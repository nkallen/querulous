package com.twitter.querulous.test

import java.sql.Connection
import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._
import com.twitter.querulous.database.Database

class FakeDatabase(connection: Connection, before: Option[String => Unit]) extends Database {
  def this(connection: Connection) = this(connection, None)
  def this(connection: Connection, before: String => Unit) = this(connection, Some(before))

  def open(): Connection = {
    before.foreach { _("open") }
    connection
  }

  def close(connection: Connection) {
    before.foreach { _("close") }
  }
}
