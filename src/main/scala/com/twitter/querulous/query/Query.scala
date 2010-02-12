package com.twitter.querulous.query

import java.sql.ResultSet

trait QueryFactory {
  def apply(connection: Connection, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def cancel()
}
