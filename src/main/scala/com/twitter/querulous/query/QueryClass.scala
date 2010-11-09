package com.twitter.querulous.query

import scala.collection.mutable

abstract class QueryClass(val name: String)

object QueryClass {
  val classes = mutable.Map[String, QueryClass]()

  /**
   * Register a query class to have timeout and cancel behavior configured.
   * You must call this for any new case objects of QueryClass.
   */
  def register(queryClasses: QueryClass*) {
    queryClasses.foreach { qc => classes(qc.name) = qc }
  }

  case object Select  extends QueryClass("select")
  case object Execute extends QueryClass("execute")

  register(Select, Execute)
}
