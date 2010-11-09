package com.twitter.querulous.query

class QueryClass(val name: String) {
  QueryClass.classes += (name -> this)
}

object QueryClass {
  val classes = scala.collection.mutable.Map[String, QueryClass]()

  object Select  extends QueryClass("select")
  object Execute extends QueryClass("execute")
}
