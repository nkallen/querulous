package com.twitter.querulous.database

import java.util.logging.{Logger, Level}
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import java.sql.{SQLException, DriverManager, Connection}
import org.apache.commons.dbcp.{PoolingDataSource, DelegatingConnection}
import org.apache.commons.pool.{PoolableObjectFactory, ObjectPool}
import com.twitter.util.Duration
import com.twitter.util.Time
import scala.annotation.tailrec
import java.lang.Thread
import java.util.concurrent.atomic.AtomicInteger

class PoolTimeoutException extends SQLException
class PoolEmptyException extends SQLException

class PooledConnection(c: Connection, p: ObjectPool) extends DelegatingConnection(c) {
  private var pool: Option[ObjectPool] = Some(p)

  private def invalidateConnection() {
    pool.foreach { _.invalidateObject(this) }
    pool = None
  }

  override def close() {
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

  private[database] def discard() {
    invalidateConnection()
    try { c.close() } catch { case _: SQLException => }
  }
}

class ThrottledPool(factory: () => Connection, val size: Int, timeout: Duration,
  idleTimeout: Duration, name: String) extends ObjectPool {
  private val pool = new LinkedBlockingQueue[(PooledConnection, Time)]()
  private val currentSize = new AtomicInteger(0)
  private val numWaiters = new AtomicInteger(0)

  try { for (i <- (0.until(size))) addObject() } catch {
    // bail until the watchdog thread repopulates.
    case e: Throwable => {
      val l = Logger.getLogger("querulous")
      l.log(Level.WARNING, "Error initially populating pool "+name, e)
    }
  }

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

  final def borrowObject(): Connection = {
    numWaiters.incrementAndGet()
    try {
      borrowObjectInternal()
    } finally {
      numWaiters.decrementAndGet()
    }
  }

  @tailrec private def borrowObjectInternal(): Connection = {
    // short circuit if the pool is empty
    if (getTotal() == 0) throw new PoolEmptyException

    val pair = pool.poll(timeout.inMillis, TimeUnit.MILLISECONDS)
    if (pair == null) throw new PoolTimeoutException
    val (connection, lastUse) = pair

    if ((Time.now - lastUse) > idleTimeout) {
      // TODO: perhaps replace with forcible termination.
      try { connection.discard() } catch { case _: SQLException => }
      // note: dbcp handles object invalidation here.
      addObjectIfEmpty()
      borrowObjectInternal()
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

  def getNumWaiters() = {
    numWaiters.get()
  }

  def invalidateObject(obj: Object) {
    currentSize.decrementAndGet()
  }

  def returnObject(obj: Object) {
    val conn = obj.asInstanceOf[PooledConnection]

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
        case t: Throwable => reportException(t)
      }
    }
  }

  def reportException(t: Throwable) {
    val thread = Thread.currentThread().getName()
    val errMsg = "%s: %s" format (t.getClass.getName, t.getMessage)

    val l = Logger.getLogger("querulous")
    l.log(Level.WARNING, "%s: Failed to add connection to the pool: %s" format (thread, errMsg), t)
  }

  // TODO: provide a reliable way to have this thread exit when shutdown is implemented
}

class ThrottledPoolingDatabaseFactory(
  serviceName: Option[String],
  size: Int,
  openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration,
  defaultUrlOptions: Map[String, String]) extends DatabaseFactory {

  def this(size: Int, openTimeout: Duration, idleTimeout: Duration, repopulateInterval: Duration,
    defaultUrlOptions: Map[String, String]) = {
    this(None, size, openTimeout, idleTimeout, repopulateInterval, defaultUrlOptions)
  }

  def this(size: Int, openTimeout: Duration, idleTimeout: Duration,
    repopulateInterval: Duration) = {
    this(size, openTimeout, idleTimeout, repopulateInterval, Map.empty)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String,
    urlOptions: Map[String, String], driverName: String) = {
    val finalUrlOptions =
      if (urlOptions eq null) {
      defaultUrlOptions
    } else {
      defaultUrlOptions ++ urlOptions
    }

    new ThrottledPoolingDatabase(serviceName, dbhosts, dbname, username, password, finalUrlOptions,
      driverName, size, openTimeout, idleTimeout, repopulateInterval)
  }
}

class ThrottledPoolingDatabase(
  val serviceName: Option[String],
  val hosts: List[String],
  val name: String,
  val username: String,
  password: String,
  val extraUrlOptions: Map[String, String],
  val driverName: String,
  numConnections: Int,
  val openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration) extends Database {

  Class.forName("com.mysql.jdbc.Driver")

  private val pool = new ThrottledPool(mkConnection, numConnections, openTimeout, idleTimeout, hosts.mkString(","))
  private val poolingDataSource = new PoolingDataSource(pool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)
  new PoolWatchdogThread(pool, hosts, repopulateInterval).start()
  private val gaugePrefix = serviceName.map{ _ + "-" }.getOrElse("")

  private val gauges = if (gaugePrefix != "") {
    List(
      (gaugePrefix + hosts.mkString(",") + "-num-connections", () => {pool.getTotal().toDouble}),
      (gaugePrefix + hosts.mkString(",") + "-num-idle-connections", () => {pool.getNumIdle().toDouble}),
      (gaugePrefix + hosts.mkString(",") + "-num-waiters", () => {pool.getNumWaiters().toDouble})
    )
  } else {
    List()
  }

  def this(hosts: List[String], name: String, username: String, password: String,
    extraUrlOptions: Map[String, String], numConnections: Int, openTimeout: Duration,
    idleTimeout: Duration, repopulateInterval: Duration) = {
    this(None, hosts, name, username, password, extraUrlOptions, Database.DEFAULT_DRIVER_NAME, numConnections, openTimeout,
      idleTimeout, repopulateInterval)
  }

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

  override protected[database] def getGauges: Seq[(String, ()=>Double)] = {
    return gauges;
  }
}
