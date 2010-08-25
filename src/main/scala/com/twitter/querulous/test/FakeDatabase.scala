package com.twitter.querulous.test

import java.sql.Connection
import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._
import com.twitter.querulous.database.Database

class FakeDatabase(connection: Connection, latency: Duration) extends Database {
  def open(): Connection = {
    Time.advance(latency)
    connection
  }

  def close(connection: Connection) {
    Time.advance(latency)
  }
}
