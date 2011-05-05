package com.twitter.querulous.database

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import java.sql.{SQLException, DriverManager, Connection}
import org.apache.commons.dbcp.PoolingDataSource
import org.apache.commons.pool.{PoolableObjectFactory, ObjectPool}
import com.twitter.querulous.PeriodicBackgroundProcess
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

class PoolTimeoutException extends Exception

class SimplePool[T <: AnyRef](factory: () => T, val size: Int, timeout: Duration) extends ObjectPool {
  private val pool = new LinkedBlockingQueue[T]()
  private val currentSize = new AtomicInteger(0)

  for (i <- (0.until(size))) addObject()

  def addObject() {
    pool.offer(factory())
    currentSize.incrementAndGet()
  }

  def borrowObject(): Object = {
    val rv = pool.poll(timeout.inMillis, TimeUnit.MILLISECONDS)
    if (rv == null) throw new PoolTimeoutException
    rv
  }

  def clear() {
    pool.clear()
  }

  def close() {
    pool.clear()
  }

  def getNumActive(): Int = {
    currentSize.get() - pool.size()
  }

  def getNumIdle(): Int = {
    pool.size()
  }

  def getTotal() = {
    currentSize.get()
  }

  def invalidateObject(obj: Object) {
    currentSize.decrementAndGet()
  }

  def returnObject(obj: Object) {
    val conn = obj.asInstanceOf[T]
    pool.offer(conn)
  }

  def setFactory(factory: PoolableObjectFactory) {
    // deprecated
  }
}

class PoolWatchdog(pool: SimplePool[_], repopulateInterval: Duration, name: String) extends PeriodicBackgroundProcess(name, repopulateInterval) {
  def periodic() {
    val delta = pool.size - pool.getTotal()
    if (delta > 0) {
      for(i <- (0.until(delta))) pool.addObject()
    }
  }
}

class SimplePoolingDatabaseFactory(
  size: Int,
  openTimeout: Duration,
  repopulateInterval: Duration,
  defaultUrlOptions: Map[String, String]) extends DatabaseFactory {

  def this(size: Int, openTimeout: Duration, repopulateInterval: Duration) = this(size, openTimeout, repopulateInterval, Map.empty)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    val finalUrlOptions = 
      if (urlOptions eq null) {
      defaultUrlOptions
    } else {
      defaultUrlOptions ++ urlOptions
    }

    new SimplePoolingDatabase(dbhosts, dbname, username, password, urlOptions, size, openTimeout, repopulateInterval)
  }
}

class SimplePoolingDatabase(
  dbhosts: List[String],
  dbname: String,
  username: String,
  password: String,
  urlOptions: Map[String, String],
  numConnections: Int,
  openTimeout: Duration,
  repopulateInterval: Duration) extends Database {

  Class.forName("com.mysql.jdbc.Driver")

  private val pool = new SimplePool(mkConnection, numConnections, openTimeout)
  private val poolingDataSource = new PoolingDataSource(pool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)
  private val watchdog = new PoolWatchdog(pool, repopulateInterval, dbhosts.mkString(","))

  def open() = {
    try {
      poolingDataSource.getConnection()
    } catch {
      case e: PoolTimeoutException =>
        throw new SqlDatabaseTimeoutException(dbhosts.mkString(",") + "/" + dbname, openTimeout)
    }
  }


  def close(connection: Connection) {
    try { connection.close() } catch { case _: SQLException => }
  }

  protected def mkConnection() = {
    DriverManager.getConnection(url(dbhosts, dbname, urlOptions), username, password)
  }
}
