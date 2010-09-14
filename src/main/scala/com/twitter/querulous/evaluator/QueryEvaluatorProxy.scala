package com.twitter.querulous.evaluator

import java.sql.ResultSet
import java.sql.{SQLException, SQLIntegrityConstraintViolationException}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.xrayspecs.{Time, Duration}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.query.Query

abstract class QueryEvaluatorProxy(queryEvaluator: QueryEvaluator) extends QueryEvaluator {
  def select[A](query: String, params: Any*)(f: ResultSet => A) = {
    delegate(queryEvaluator.select(query, params: _*)(f))
  }

  def selectOne[A](query: String, params: Any*)(f: ResultSet => A) = {
    delegate(queryEvaluator.selectOne(query, params: _*)(f))
  }

  def execute(query: String, params: Any*) = {
    delegate(queryEvaluator.execute(query, params: _*))
  }

  def executeBatch(query: String)(f: Query => Unit) = {
    delegate(queryEvaluator.executeBatch(query)(f))
  }

  def count(query: String, params: Any*) = {
    delegate(queryEvaluator.count(query, params: _*))
  }

  def nextId(tableName: String) = {
    delegate(queryEvaluator.nextId(tableName))
  }

  def insert(query: String, params: Any*) = {
    delegate(queryEvaluator.insert(query, params: _*))
  }

  def transaction[T](f: Transaction => T) = {
    delegate(queryEvaluator.transaction(f))
  }

  protected def delegate[A](f: => A) = f
}
