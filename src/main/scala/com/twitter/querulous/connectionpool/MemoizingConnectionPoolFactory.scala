package com.twitter.querulous.connectionpool

import scala.collection.mutable

class MemoizingConnectionPoolFactory(connectionPoolFactory: ConnectionPoolFactory) extends ConnectionPoolFactory {
  private val connectionPools = new mutable.HashMap[String, ConnectionPool]

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = synchronized {
    connectionPools.getOrElseUpdate(
      dbhosts.first + "/" + dbname,
      connectionPoolFactory(dbhosts, dbname, username, password))
  }

  def close() {
    for ((name, pool) <- connectionPools) pool.close()
    connectionPools.clear()
  }
}
