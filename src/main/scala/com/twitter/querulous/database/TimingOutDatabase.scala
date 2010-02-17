package com.twitter.querulous.database

import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration
import java.sql.Connection

class TimingOutDatabaseFactory(databaseFactory: DatabaseFactory, poolSize: Int, timeout: Duration) extends DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database = {
    new TimingOutDatabase(databaseFactory(dbhosts, dbname, username, password), poolSize, timeout)
  }
}

class TimingOutDatabase(database: Database, poolSize: Int, timeout: Duration) extends Database {
  private val executor = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, new SynchronousQueue[Runnable])
 
  override def open(): Connection = {
    val future = new FutureTask(new Callable[Connection] {
      def call = {
        database.open()
      }
    })
    try {
      executor.execute(future)
      future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: JTimeoutException => throw new TimeoutException
      case e: RejectedExecutionException => throw new TimeoutException
    }
  }

  def close(connection: Connection) { database.close(connection) }
}
