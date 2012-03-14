package com.twitter.querulous.test

import java.sql.ResultSet
import com.twitter.querulous.evaluator.{Transaction, QueryEvaluator, ParamsApplier}
import com.twitter.querulous.query.{QueryClass}


class FakeQueryEvaluator[A](trans: Transaction, resultSets: Seq[ResultSet]) extends QueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = resultSets.map(f)
  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = None
  def count(queryClass: QueryClass, query: String, params: Any*) = 0
  def execute(queryClass: QueryClass, query: String, params: Any*) = 0
  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit) = 0
  def nextId(tableName: String) = 0
  def insert(queryClass: QueryClass, query: String, params: Any*) = 0
  def transaction[T](f: Transaction => T) = f(trans)
}
