package com.twitter.querulous.database

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import java.sql.{SQLException, DriverManager, Connection}
import org.apache.commons.dbcp.{PoolingDataSource, DelegatingConnection}
import org.apache.commons.pool.{PoolableObjectFactory, ObjectPool}
import com.twitter.util.Duration
import com.twitter.util.Time
import scala.annotation.tailrec
import java.lang.Thread

class PoolTimeoutException extends SQLException
class PoolEmptyException extends SQLException

class PooledConnection(c: Connection, p: ObjectPool) extends DelegatingConnection(c) {
  private var pool: Option[ObjectPool] = Some(p)

  private def invalidateConnection() {
    pool.foreach { _.invalidateObject(this) }
    pool = None
  }

  override def close() = synchronized {
    val isClosed = try { c.isClosed() } catch {
      case e: Exception => {
        invalidateConnection()
        throw e
      }
    }

    if (!isClosed) {
      pool match {
        case Some(pl) => pl.returnObject(this)
        case None =>
          passivate()
          c.close()
      }
    } else {
      invalidateConnection()
      throw new SQLException("Already closed.")
    }
  }
}

class ThrottledPool(factory: () => Connection, val size: Int, timeout: Duration, idleTimeout: Duration) extends ObjectPool {
  private val pool = new LinkedBlockingQueue[(Connection, Time)]()
  private val currentSize = new AtomicInteger(0)

  for (i <- (0.until(size))) addObject()

  def addObject() {
    pool.offer((new PooledConnection(factory(), this), Time.now))
    currentSize.incrementAndGet()
  }

  def addObjectIfEmpty() = synchronized {
    if (getTotal() == 0) addObject()
  }

  def addObjectUnlessFull() = synchronized {
    if (getTotal() < size) {
      addObject()
    }
  }

  @tailrec final def borrowObject(): Connection = {
    // short circuit if the pool is empty
    if (getTotal() == 0) throw new PoolEmptyException

    val pair = pool.poll(timeout.inMillis, TimeUnit.MILLISECONDS)
    if (pair == null) throw new PoolTimeoutException
    val (connection, lastUse) = pair

    if ((Time.now - lastUse) > idleTimeout) {
      // TODO: perhaps replace with forcible termination.
      try { connection.close() } catch { case _: SQLException => }
      // note: dbcp handles object invalidation here.
      addObjectIfEmpty()
      borrowObject()
    } else {
      connection
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

class PoolWatchdogThread(
  pool: ThrottledPool,
  hosts: Seq[String],
  repopulateInterval: Duration) extends Thread(hosts.mkString(",") + "-pool-watchdog") {

  this.setDaemon(true)

  override def run() {
    var lastTimePoolPopulated = Time.now
    while(true) {
      try {
        val timeToSleepInMills = (repopulateInterval - (Time.now - lastTimePoolPopulated)).inMillis
        if (timeToSleepInMills > 0) {
          Thread.sleep(timeToSleepInMills)
        }
        lastTimePoolPopulated = Time.now
        pool.addObjectUnlessFull()
      } catch {
        case t: Throwable => {
          System.err.println(Time.now.format("yyyy-MM-dd HH:mm:ss Z") + ": " +
                             Thread.currentThread().getName() +
                             " failed to add connection to the pool")
          t.printStackTrace(System.err)
        }
      }
    }
  }

  // TODO: provide a reliable way to have this thread exit when shutdown is implemented
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

    new ThrottledPoolingDatabase(dbhosts, dbname, username, password, finalUrlOptions, size, openTimeout, idleTimeout, repopulateInterval)
  }
}

class ThrottledPoolingDatabase(
  val hosts: List[String],
  val name: String,
  val username: String,
  password: String,
  val extraUrlOptions: Map[String, String],
  numConnections: Int,
  val openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration) extends Database {

  Class.forName("com.mysql.jdbc.Driver")

  private val pool = new ThrottledPool(mkConnection, numConnections, openTimeout, idleTimeout)
  private val poolingDataSource = new PoolingDataSource(pool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)
  new PoolWatchdogThread(pool, hosts, repopulateInterval).start()

  def open() = {
    try {
      poolingDataSource.getConnection()
    } catch {
      case e: PoolTimeoutException =>
        throw new SqlDatabaseTimeoutException(hosts.mkString(",") + "/" + name, openTimeout)
    }
  }

  def close(connection: Connection) {
    try { connection.close() } catch { case _: SQLException => }
  }

  protected def mkConnection(): Connection = {
    DriverManager.getConnection(url(hosts, name, urlOptions), username, password)
  }
}
