package com.twitter.querulous.async

import java.util.concurrent.Executors
import java.sql.ResultSet
import com.twitter.util.{Future, FuturePool}
import com.twitter.querulous.config.{Connection => ConnectionConfig}
import com.twitter.querulous.DaemonThreadFactory
import com.twitter.querulous.evaluator._
import com.twitter.querulous.query.{QueryClass, SqlQueryFactory}
import com.twitter.querulous.database.ThrottledPoolingDatabaseFactory
import com.twitter.conversions.time._


object AsyncQueryEvaluator extends AsyncQueryEvaluatorFactory {
  lazy val defaultFuturePool = FuturePool(Executors.newCachedThreadPool(new DaemonThreadFactory))

  private def createEvaluatorFactory() = {
    new StandardAsyncQueryEvaluatorFactory(
      new BlockingDatabaseWrapperFactory(
        defaultFuturePool,
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
    urlOptions: Map[String, String]
  ): AsyncQueryEvaluator = {
    createEvaluatorFactory()(dbhosts, dbname, username, password, urlOptions)
  }
}

trait AsyncQueryEvaluatorFactory {
  def apply(
    dbhosts: List[String],
    dbname: String,
    username: String,
    password: String,
    urlOptions: Map[String, String]
  ): AsyncQueryEvaluator

  def apply(dbhost: String, dbname: String, username: String, password: String, urlOptions: Map[String, String]): AsyncQueryEvaluator = {
    apply(List(dbhost), dbname, username, password, urlOptions)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(dbhosts, dbname, username, password, Map[String,String]())
  }

  def apply(dbhost: String, dbname: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(List(dbhost), dbname, username, password, Map[String,String]())
  }

  def apply(dbhost: String, username: String, password: String): AsyncQueryEvaluator = {
    apply(List(dbhost), null, username, password, Map[String,String]())
  }

  def apply(dbhosts: List[String], username: String, password: String): AsyncQueryEvaluator = {
    apply(dbhosts, null, username, password, Map[String,String]())
  }

  def apply(connection: ConnectionConfig): AsyncQueryEvaluator = {
    apply(connection.hostnames.toList, connection.database, connection.username, connection.password, connection.urlOptions)
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
