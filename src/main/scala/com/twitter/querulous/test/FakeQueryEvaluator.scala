package com.twitter.querulous.test

import java.sql.ResultSet
import com.twitter.xrayspecs.Duration
import com.twitter.querulous.evaluator.{Transaction, QueryEvaluator}
import com.twitter.querulous.query.Query

class FakeQueryEvaluator[A](trans: Transaction, resultSets: Seq[ResultSet]) extends QueryEvaluator {
  def select[A](query: String, params: Any*)(f: ResultSet => A) = resultSets.map(f)
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A) = None
  def count(query: String, params: Any*) = 0
  def execute(query: String, params: Any*) = 0
  def executeBatch(query: String)(f: Query => Unit) = 0
  def nextId(tableName: String) = 0
  def insert(query: String, params: Any*) = 0
  def transaction[T](f: Transaction => T) = f(trans)
}
