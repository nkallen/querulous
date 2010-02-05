package com.twitter.querulous.connectionpool

import java.sql.{Connection, SQLException}
import org.apache.commons.dbcp.{PoolableConnectionFactory, DriverManagerConnectionFactory, PoolingDataSource}
import org.apache.commons.pool.impl.{GenericObjectPool, StackKeyedObjectPoolFactory}
import com.twitter.xrayspecs.Duration

class ApacheConnectionPoolFactory(
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  maxWaitForConnectionReservation: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration) extends ConnectionPoolFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    val pool = new ApacheConnectionPool(
      dbhosts,
      dbname,
      username,
      password,
      minOpenConnections,
      maxOpenConnections,
      checkConnectionHealthWhenIdleFor,
      maxWaitForConnectionReservation,
      checkConnectionHealthOnReservation,
      evictConnectionIfIdleFor)
    pool
  }
}

class ApacheConnectionPool(
  dbhosts: List[String],
  dbname: String,
  username: String,
  password: String,
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  maxWaitForConnectionReservation: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration) extends ConnectionPool {

  Class.forName("com.mysql.jdbc.Driver")

  private val config = new GenericObjectPool.Config
  config.maxActive = maxOpenConnections
  config.maxIdle = maxOpenConnections
  config.minIdle = minOpenConnections
  config.maxWait = maxWaitForConnectionReservation.inMillis

  config.timeBetweenEvictionRunsMillis = checkConnectionHealthWhenIdleFor.inMillis
  config.testWhileIdle = true
  config.testOnBorrow = checkConnectionHealthOnReservation
  config.minEvictableIdleTimeMillis = evictConnectionIfIdleFor.inMillis

  private val connectionPool = new GenericObjectPool(null, config)
  private val connectionFactory = new DriverManagerConnectionFactory(url(dbhosts, dbname), username, password)
  private val poolableConnectionFactory = new PoolableConnectionFactory(
    connectionFactory,
    connectionPool,
    null,
    "/* ping */ SELECT 1",
    false,
    true)
  private val poolingDataSource = new PoolingDataSource(connectionPool)

  def release(connection: Connection) {
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }
  }

  def reserve() = {
    poolingDataSource.getConnection()
  }

  def close() {
    connectionPool.close()
  }

  override def toString = dbhosts.first + "_" + dbname
}
