package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}
import scala.collection.immutable.Map

trait QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, queryString: String, params: Any*): Query
  def shutdown() = {}
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def addParams(params: Any*)
  def cancel()

  /**
   * Add an annotation you want to be sent along with the query as a comment.
   * Could for example be information you want to find in a slow query log.
   */
  private var ann = Map[String, String]()
  def addAnnotation(key: String, value:String) = ann = ann + (key -> value)
  def annotations: Map[String, String] = ann
}
