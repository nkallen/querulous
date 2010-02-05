package com.twitter.querulous.query

import java.sql.ResultSet

abstract class QueryProxy(query: Query) extends Query {
  def select[A](f: ResultSet => A) = delegate(query.select(f))

  def execute() = delegate(query.execute())

  def cancel() = query.cancel()

  protected def delegate[A](f: => A) = f
}
