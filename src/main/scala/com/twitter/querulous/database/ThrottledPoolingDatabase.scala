package com.twitter.querulous.database

import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import java.sql.{SQLException, DriverManager, Connection}
import org.apache.commons.dbcp.{PoolableConnection, PoolingDataSource}
import org.apache.commons.pool.{PoolableObjectFactory, ObjectPool}
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import scala.annotation.tailrec

class PoolTimeoutException extends SQLException

class ThrottledPool(factory: () => Connection, val size: Int, timeout: Duration, idleTimeout: Duration) extends ObjectPool {
  private val pool = new LinkedBlockingQueue[(Connection, Time)]()
  private val currentSize = new AtomicInteger(0)

  for (i <- (0.until(size))) addObject()

  def addObject() {
    pool.offer((factory(), Time.now))
    currentSize.incrementAndGet()
  }

  def addObjectIfEmpty() = synchronized {
    if (getTotal() == 0) addObject()
  }

  def addObjectUnlessFull() = synchronized {
    if (getTotal() < size) addObject()
  }

  @tailrec final def borrowObject(): Connection = {
    val pair = pool.poll(timeout.inMillis, TimeUnit.MILLISECONDS)
    if (pair == null) throw new PoolTimeoutException
    val (connection, lastUse) = pair

    if ((Time.now - lastUse) > idleTimeout) {
      // TODO: perhaps replace with forcible termination.
      try { connection.close() } catch { case _: SQLException => }
      invalidateObject(connection)
      addObjectIfEmpty()
      borrowObject()
    } else {
      new PoolableConnection(connection, this)
    }
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
    val conn = obj.asInstanceOf[Connection]
    pool.offer((conn, Time.now))
  }

  def setFactory(factory: PoolableObjectFactory) {
    // deprecated
  }
}

class PoolWatchdog(pool: ThrottledPool) extends TimerTask {
  def run() { pool.addObjectUnlessFull() }
}

class ThrottledPoolingDatabaseFactory(
  size: Int,
  openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration,
  defaultUrlOptions: Map[String, String]) extends DatabaseFactory {

  def this(
    size: Int,
    openTimeout: Duration,
    idleTimeout: Duration,
    repopulateInterval: Duration) = this(size, openTimeout, idleTimeout, repopulateInterval, Map.empty)

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    val finalUrlOptions =
      if (urlOptions eq null) {
      defaultUrlOptions
    } else {
      defaultUrlOptions ++ urlOptions
    }

    new ThrottledPoolingDatabase(dbhosts, dbname, username, password, urlOptions, size, openTimeout, idleTimeout, repopulateInterval)
  }
}

class ThrottledPoolingDatabase(
  dbhosts: List[String],
  dbname: String,
  username: String,
  password: String,
  urlOptions: Map[String, String],
  numConnections: Int,
  openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration) extends Database {

  Class.forName("com.mysql.jdbc.Driver")

  private val pool = new ThrottledPool(mkConnection, numConnections, openTimeout, idleTimeout)
  private val poolingDataSource = new PoolingDataSource(pool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)
  private val watchdogTask = new PoolWatchdog(pool)
  private val watchdog = new Timer(dbhosts.mkString(",") + "-pool-watchdog", true)
  watchdog.scheduleAtFixedRate(watchdogTask, 0, repopulateInterval.inMillis)

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

  protected def mkConnection(): Connection = {
    DriverManager.getConnection(url(dbhosts, dbname, urlOptions), username, password)
  }
}
