package com.twitter.querulous.evaluator

import java.sql.ResultSet
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.xrayspecs.TimeConversions._


object QueryEvaluator extends QueryEvaluatorFactory {
  private def createEvaluatorFactory() = {
    val queryFactory = new SqlQueryFactory
    val databaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
    new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    createEvaluatorFactory()(dbhosts, dbname, username, password)
  }

  def apply(dbhosts: List[String], username: String, password: String) = {
    createEvaluatorFactory()(dbhosts, username, password)
  }
}

trait QueryEvaluatorFactory {
  def apply(dbhost: String, dbname: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password)
  }
  def apply(dbhost: String, username: String, password: String): QueryEvaluator = apply(List(dbhost), username, password)
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): QueryEvaluator
  def apply(dbhosts: List[String], username: String, password: String): QueryEvaluator
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
