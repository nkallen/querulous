package com.twitter.querulous.query

import java.sql.ResultSet

abstract class QueryProxy(query: Query) extends Query {
  def select[A](f: ResultSet => A) = delegate(query.select(f))

  def execute() = delegate(query.execute())

  def cancel() = query.cancel()

  def addParams(params: Any*) = query.addParams(params)

  protected def delegate[A](f: => A) = f

  /**
   * If this query wraps another query, return it.
   * For example a TimingOutQuery could wrap a SqlQuery.
   */
  protected def getRootQuery: Query = {
    query match {
      case q: QueryProxy => q.getRootQuery
      case _ => query
    }
  }
}
