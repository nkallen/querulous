package com.twitter.querulous.evaluator

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException, Connection}
import com.twitter.querulous.query.{QueryClass, QueryFactory}

class Transaction(queryFactory: QueryFactory, connection: Connection) extends QueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    queryFactory(connection, queryClass, query, params: _*).select(f)
  }

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    select(queryClass, query, params: _*)(f).headOption
  }

  def count(queryClass: QueryClass, query: String, params: Any*) = {
    selectOne(queryClass, query, params: _*)(_.getInt("count(*)")) getOrElse 0
  }

  def execute(queryClass: QueryClass, query: String, params: Any*) = {
    queryFactory(connection, queryClass, query, params: _*).execute()
  }

  def executeBatch(queryClass: QueryClass, queryString: String)(f: ParamsApplier => Unit) = {
    val query = queryFactory(connection, queryClass, queryString)
    f(new ParamsApplier(query))
    query.execute()
  }

  def nextId(tableName: String) = {
    execute("UPDATE " + tableName + " SET id=LAST_INSERT_ID(id+1)")
    selectOne("SELECT LAST_INSERT_ID()") { _.getLong("LAST_INSERT_ID()") } getOrElse 0L
  }

  def insert(queryClass: QueryClass, query: String, params: Any*): Long = {
    execute(queryClass, query, params: _*)
    selectOne("SELECT LAST_INSERT_ID()") { _.getLong("LAST_INSERT_ID()") } getOrElse {
      throw new SQLIntegrityConstraintViolationException
    }
  }

  def begin() = {
    connection.setAutoCommit(false)
  }

  def commit() = {
    connection.commit()
    connection.setAutoCommit(true)
  }

  def rollback() = {
    connection.rollback()
    try {
      connection.setAutoCommit(true)
    } catch {
      case _: SQLException => // it's ok. the connection may be dead.
    }
  }

  def transaction[T](f: Transaction => T) = f(this)
}
