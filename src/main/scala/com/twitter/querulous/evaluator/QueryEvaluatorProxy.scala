package com.twitter.querulous.evaluator

import java.sql.ResultSet
import java.sql.{SQLException, SQLIntegrityConstraintViolationException}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._
import com.twitter.querulous.query.{QueryClass, Query}


abstract class QueryEvaluatorProxy(queryEvaluator: QueryEvaluator) extends QueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    delegate(queryEvaluator.select(queryClass, query, params: _*)(f))
  }

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    delegate(queryEvaluator.selectOne(queryClass, query, params: _*)(f))
  }

  def execute(queryClass: QueryClass, query: String, params: Any*) = {
    delegate(queryEvaluator.execute(queryClass, query, params: _*))
  }

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit) = {
    delegate(queryEvaluator.executeBatch(queryClass, query)(f))
  }

  def count(queryClass: QueryClass, query: String, params: Any*) = {
    delegate(queryEvaluator.count(queryClass, query, params: _*))
  }

  def nextId(tableName: String) = {
    delegate(queryEvaluator.nextId(tableName))
  }

  def insert(queryClass: QueryClass, query: String, params: Any*) = {
    delegate(queryEvaluator.insert(query, params: _*))
  }

  def transaction[T](f: Transaction => T) = {
    delegate(queryEvaluator.transaction(f))
  }

  protected def delegate[A](f: => A) = f
}
