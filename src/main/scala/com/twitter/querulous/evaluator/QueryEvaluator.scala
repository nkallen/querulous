package com.twitter.querulous.evaluator

import java.sql.ResultSet
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.connectionpool.ApacheConnectionPoolFactory
import com.twitter.xrayspecs.TimeConversions._

object QueryEvaluator extends QueryEvaluatorFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    val queryFactory = new SqlQueryFactory
    val connectionPoolFactory = new ApacheConnectionPoolFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
    val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(connectionPoolFactory, queryFactory)
    queryEvaluatorFactory(dbhosts, dbname, username, password)
  }
}

trait QueryEvaluatorFactory {
  def apply(dbhost: String, dbname: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password)
  }
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): QueryEvaluator
}

trait QueryEvaluator {
  def select[A](query: String, params: Any*)(f: ResultSet => A): Seq[A]
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Option[A]
  def count(query: String, params: Any*): Int
  def execute(query: String, params: Any*): Int
  def nextId(tableName: String): Long
  def insert(query: String, params: Any*): Int
  def transaction[T](f: Transaction => T): T
}
