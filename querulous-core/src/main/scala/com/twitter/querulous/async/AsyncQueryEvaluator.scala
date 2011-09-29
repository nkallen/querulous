package com.twitter.querulous.async

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import java.sql.ResultSet
import com.twitter.util.{Future, FuturePool}
import com.twitter.querulous.config.{Connection => ConnectionConfig}
import com.twitter.querulous.DaemonThreadFactory
import com.twitter.querulous.evaluator._
import com.twitter.querulous.query.{QueryClass, SqlQueryFactory}
import com.twitter.querulous.database.{ThrottledPoolingDatabaseFactory, Database}
import com.twitter.conversions.time._


object AsyncQueryEvaluator extends AsyncQueryEvaluatorFactory {
  lazy val defaultWorkPool = FuturePool(Executors.newCachedThreadPool(new DaemonThreadFactory))
  lazy val defaultMaxWaiters = Int.MaxValue

  def checkoutPool(maxWaiters: Int) = {
    FuturePool(
      new ThreadPoolExecutor(
        1, /* min size */
        1, /* max size */
        0, /* ignored, since the sizes are the same */
        TimeUnit.MILLISECONDS, /* similarly ignored */
        new LinkedBlockingQueue(maxWaiters)))
  }

  private def createEvaluatorFactory() = {
    new StandardAsyncQueryEvaluatorFactory(
      new BlockingDatabaseWrapperFactory(
        defaultWorkPool,
        checkoutPool(defaultMaxWaiters),
        new ThrottledPoolingDatabaseFactory(10, 100.millis, 10.seconds, 1.second)
      ),
      new SqlQueryFactory
    )
  }

  def apply(
    dbhosts: List[String],
    dbname: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driverName: String
  ): AsyncQueryEvaluator = {
    createEvaluatorFactory()(dbhosts, dbname, username, password, urlOptions, driverName)
  }
}

trait AsyncQueryEvaluatorFactory {
  def apply(
    dbhosts: List[String],
    dbname: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driverName: String
  ): AsyncQueryEvaluator

  def apply(dbhost: String, dbname: String, username: String, password: String, urlOptions: Map[String, String]): AsyncQueryEvaluator = {
    apply(List(dbhost), dbname, username, password, urlOptions, Database.DEFAULT_DRIVER_NAME)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): AsyncQueryEvaluator = {
    apply(dbhosts, dbname, username, password, urlOptions, Database.DEFAULT_DRIVER_NAME)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(dbhosts, dbname, username, password, Map[String,String](), Database.DEFAULT_DRIVER_NAME)
  }

  def apply(dbhost: String, dbname: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(List(dbhost), dbname, username, password, Map[String,String](), Database.DEFAULT_DRIVER_NAME)
  }

  def apply(dbhost: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(List(dbhost), null, username, password, Map[String,String](), Database.DEFAULT_DRIVER_NAME)
  }

  def apply(dbhosts: List[String], username: String, password: String): AsyncQueryEvaluator = {
    apply(dbhosts, null, username, password, Map[String,String](), Database.DEFAULT_DRIVER_NAME)
  }

  def apply(connection: ConnectionConfig): AsyncQueryEvaluator = {
    apply(connection.hostnames.toList, connection.database, connection.username, connection.password, connection.urlOptions, connection.driverName)
  }
}

trait AsyncQueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Future[Seq[A]]

  def select[A](query: String, params: Any*)(f: ResultSet => A): Future[Seq[A]] = {
    select(QueryClass.Select, query, params: _*)(f)
  }

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Future[Option[A]]

  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Future[Option[A]] = {
    selectOne(QueryClass.Select, query, params: _*)(f)
  }

  def count(queryClass: QueryClass, query: String, params: Any*): Future[Int]

  def count(query: String, params: Any*): Future[Int] = {
    count(QueryClass.Select, query, params: _*)
  }

  def execute(queryClass: QueryClass, query: String, params: Any*): Future[Int]

  def execute(query: String, params: Any*): Future[Int] = {
    execute(QueryClass.Execute, query, params: _*)
  }

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit): Future[Int]

  def executeBatch(query: String)(f: ParamsApplier => Unit): Future[Int] = {
    executeBatch(QueryClass.Execute, query)(f)
  }

  def nextId(tableName: String): Future[Long]

  def insert(queryClass: QueryClass, query: String, params: Any*): Future[Long]

  def insert(query: String, params: Any*): Future[Long] = {
    insert(QueryClass.Execute, query, params: _*)
  }

  def transaction[T](f: Transaction => T): Future[T]
}
