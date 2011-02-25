package com.twitter.querulous.query

import com.twitter.querulous._
import java.sql.{ResultSet, Connection}
import scala.collection.mutable
import com.twitter.querulous.StatsCollector
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

trait QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def addParams(params: Any*)
  def cancel()
}
