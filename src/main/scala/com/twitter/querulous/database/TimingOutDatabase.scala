package com.twitter.querulous.database

import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration

class TimingOutDatabaseFactory(databaseFactory: DatabaseFactory, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database = {
    new TimingOutDatabase(databaseFactory(dbhosts, dbname, username, password), dbhosts, dbname, poolSize, queueSize, openTimeout, initialTimeout)
  }
}

class TimingOutDatabase(database: Database, dbhosts: List[String], dbname: String, poolSize: Int, queueSize: Int, openTimeout: Duration, initialTimeout: Duration) extends Database {
  private val dbOpen = new Callable[Connection] { def call = database.open() }
  private val queue = new LinkedBlockingQueue[Runnable](queueSize)
  private val executor = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, queue)
  greedilyInstantiateConnections()

  private def greedilyInstantiateConnections(): Unit = {
    val future = new FutureTask(dbOpen)
    executor.execute(future)
    close(future.get(initialTimeout.inMillis, TimeUnit.MILLISECONDS))
  }

  override def open(): Connection = {
    val future = new FutureTask(dbOpen)
    try {
      executor.execute(future)
      future.get(openTimeout.inMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: JTimeoutException => throw new TimeoutException(dbhosts.mkString(",")+"/"+dbname)
      case e: RejectedExecutionException => throw new TimeoutException(dbhosts.mkString(",")+"/"+dbname)
    }
  }

  def close(connection: Connection) { database.close(connection) }
}
