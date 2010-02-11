package com.twitter.querulous.fake

import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.database.Database

class FakeDatabase(connection: Connection, latency: Duration) extends Database {
  def reserve(): Connection = {
    Time.advance(latency)
    connection
  }

  def release(connection: Connection) {
    Time.advance(latency)
  }

  def close() = {}
}
