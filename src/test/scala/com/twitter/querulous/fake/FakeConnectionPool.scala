package com.twitter.querulous.fake

import java.sql.Connection
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.connectionpool.ConnectionPool

class FakeConnectionPool(connection: Connection, latency: Duration) extends ConnectionPool {
  def reserve(): Connection = {
    Time.advance(latency)
    connection
  }

  def release(connection: Connection) {
    Time.advance(latency)
  }

  def close() = {}
}
