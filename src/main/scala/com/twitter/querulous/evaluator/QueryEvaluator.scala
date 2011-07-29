package com.twitter.querulous.evaluator

import com.twitter.querulous._
import java.sql.ResultSet
import com.twitter.conversions.time._
import com.twitter.querulous.database._
import com.twitter.querulous.query._


object QueryEvaluator extends QueryEvaluatorFactory {
  private def createEvaluatorFactory() = {
    val queryFactory = new SqlQueryFactory
    val databaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
    new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    createEvaluatorFactory()(dbhosts, dbname, username, password, urlOptions)
  }
}

trait QueryEvaluatorFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator

  def apply(dbhost: String, dbname: String, username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password, urlOptions)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): QueryEvaluator = {
    apply(dbhosts, dbname, username, password, Map[String,String]())
  }

  def apply(dbhost: String, dbname: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password, Map[String,String]())
  }

  def apply(dbhost: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), null, username, password, Map[String,String]())
  }

  def apply(dbhosts: List[String], username: String, password: String): QueryEvaluator = {
    apply(dbhosts, null, username, password, Map[String,String]())
  }

  def apply(connection: config.Connection): QueryEvaluator = {
    apply(connection.hostnames.toList, connection.database, connection.username, connection.password, connection.urlOptions)
  }
}

class ParamsApplier(query: Query) {
  def apply(params: Any*) = query.addParams(params: _*)
}

trait QueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Seq[A]
  def select[A](query: String, params: Any*)(f: ResultSet => A): Seq[A] =
    select(QueryClass.Select, query, params: _*)(f)

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Option[A]
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Option[A] =
    selectOne(QueryClass.Select, query, params: _*)(f)

  def count(queryClass: QueryClass, query: String, params: Any*): Int
  def count(query: String, params: Any*): Int =
    count(QueryClass.Select, query, params: _*)

  def execute(queryClass: QueryClass, query: String, params: Any*): Int
  def execute(query: String, params: Any*): Int =
    execute(QueryClass.Execute, query, params: _*)

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit): Int
  def executeBatch(query: String)(f: ParamsApplier => Unit): Int =
    executeBatch(QueryClass.Execute, query)(f)

  def nextId(tableName: String): Long

  def insert(queryClass: QueryClass, query: String, params: Any*): Long
  def insert(query: String, params: Any*): Long =
    insert(QueryClass.Execute, query, params: _*)

  def transaction[T](f: Transaction => T): T
}
