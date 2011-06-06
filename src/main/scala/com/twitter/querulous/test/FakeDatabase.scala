package com.twitter.querulous.test

import java.sql.Connection
import com.twitter.util.{Duration, Time}
import com.twitter.conversions.time._
import com.twitter.querulous.database.Database

trait FakeDatabase extends Database {
  val hosts           = List("fakehost")
  val name            = "fake"
  val username        = "fakeuser"
  val openTimeout     = 500.millis
  val extraUrlOptions: Map[String,String] = Map.empty
}

class FakeDBConnectionWrapper(connection: Connection, before: Option[String => Unit])
extends Database
with FakeDatabase {
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
