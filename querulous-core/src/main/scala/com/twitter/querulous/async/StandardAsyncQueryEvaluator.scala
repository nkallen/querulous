package com.twitter.querulous.async

import java.sql.ResultSet
import com.twitter.util.Future
import com.twitter.querulous.query.{QueryClass, QueryFactory}
import com.twitter.querulous.evaluator.{Transaction, ParamsApplier}


class StandardAsyncQueryEvaluatorFactory(
  databaseFactory: AsyncDatabaseFactory,
  queryFactory: QueryFactory)
extends AsyncQueryEvaluatorFactory {
    def apply(
    hosts: List[String],
    name: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driverName: String
  ): AsyncQueryEvaluator = {
    new StandardAsyncQueryEvaluator(
      databaseFactory(hosts, name, username, password, urlOptions, driverName),
      queryFactory
    )
  }
}

class StandardAsyncQueryEvaluator(val database: AsyncDatabase, queryFactory: QueryFactory)
extends AsyncQueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    withTransaction(_.select(queryClass, query, params: _*)(f))
  }

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    withTransaction(_.selectOne(queryClass, query, params: _*)(f))
  }

  def count(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.count(queryClass, query, params: _*))
  }

  def execute(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.execute(queryClass, query, params: _*))
  }

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit) = {
    withTransaction(_.executeBatch(queryClass, query)(f))
  }

  def nextId(tableName: String) = {
    withTransaction(_.nextId(tableName))
  }

  def insert(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.insert(queryClass, query, params: _*))
  }

  def transaction[T](f: Transaction => T) = {
    withTransaction { transaction =>
      transaction.begin()
      try {
        val rv = f(transaction)
        transaction.commit()
        rv
      } catch {
        case e: Throwable => {
          try {
            transaction.rollback()
          } catch { case _ => () }
          throw e
        }
      }
    }
  }

  private def withTransaction[R](f: Transaction => R): Future[R] = {
    database.withConnection { c => f(new Transaction(queryFactory, c)) }
  }

  // equality overrides

  override def equals(other: Any) = other match {
    case other: StandardAsyncQueryEvaluator => database eq other.database
    case _ => false
  }

  override def hashCode = database.hashCode
}
