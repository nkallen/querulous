package com.twitter.querulous.connectionpool

import java.sql.Connection

class StatsCollectingConnectionPoolFactory(
  connectionPoolFactory: ConnectionPoolFactory,
  stats: StatsCollector) extends ConnectionPoolFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): ConnectionPool = {
    new StatsCollectingConnectionPool(connectionPoolFactory(dbhosts, dbname, username, password), stats)
  }
}

class StatsCollectingConnectionPool(pool: ConnectionPool, stats: StatsCollector)
  extends ConnectionPool {

  override def reserve(): Connection = {
    stats.time("connection-pool-reserve-timing") {
      pool.reserve()
    }
  }

  override def release(connection: Connection) = {
    stats.time("connection-pool-release-timing") {
      pool.release(connection)
    }
  }

  override def close() = {
    pool.close()
  }
}
