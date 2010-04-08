package com.twitter.querulous.evaluator

import java.sql.ResultSet
import org.apache.commons.dbcp.{DriverManagerConnectionFactory, PoolableConnectionFactory, PoolingDataSource}
import org.apache.commons.pool.impl.GenericObjectPool
import com.twitter.querulous.database.{Database, DatabaseFactory}
import com.twitter.querulous.query.QueryFactory

class StandardQueryEvaluatorFactory(
  databaseFactory: DatabaseFactory,
  queryFactory: QueryFactory) extends QueryEvaluatorFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    val database = databaseFactory(dbhosts, dbname, username, password)
    new StandardQueryEvaluator(database, queryFactory)
  }

  def apply(dbhosts: List[String], username: String, password: String) = {
    val database = databaseFactory(dbhosts, username, password)
    new StandardQueryEvaluator(database, queryFactory)
  }
}

class StandardQueryEvaluator(protected val database: Database, queryFactory: QueryFactory)
  extends QueryEvaluator {

  def select[A](query: String, params: Any*)(f: ResultSet => A) = withTransaction(_.select(query, params: _*)(f))
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A) = withTransaction(_.selectOne(query, params: _*)(f))
  def count(query: String, params: Any*) = withTransaction(_.count(query, params: _*))
  def execute(query: String, params: Any*) = withTransaction(_.execute(query, params: _*))
  def nextId(tableName: String) = withTransaction(_.nextId(tableName))
  def insert(query: String, params: Any*) = withTransaction(_.insert(query, params: _*))

  def transaction[T](f: Transaction => T) = {
    withTransaction { transaction =>
      transaction.begin()
      try {
        val rv = f(transaction)
        transaction.commit()
        rv
      } catch {
        case e: Throwable =>
          transaction.rollback()
          throw e
      }
    }
  }

  private def withTransaction[A](f: Transaction => A) = {
    database.withConnection { connection => f(new Transaction(queryFactory, connection)) }
  }

  override def equals(other: Any) = {
    other match {
      case other: StandardQueryEvaluator =>
        database eq other.database
      case _ =>
        false
    }
  }

  override def hashCode = database.hashCode
}
