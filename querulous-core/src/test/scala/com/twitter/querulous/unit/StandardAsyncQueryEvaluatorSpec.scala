package com.twitter.querulous.unit

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import java.sql.{Connection, ResultSet}
import com.twitter.conversions.time._
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.evaluator.{QueryEvaluator, ParamsApplier, Transaction}
import com.twitter.querulous.database.Database
import com.twitter.querulous.query._
import com.twitter.querulous.async._

class StandardAsyncQueryEvaluatorSpec extends Specification with JMocker with ClassMocker {

  val workPool = AsyncQueryEvaluator.defaultWorkPool

  val database     = mock[Database]
  val connection   = mock[Connection]
  val query        = mock[Query]
  val queryFactory = mock[QueryFactory]

  def newEvaluator() = {
    new StandardAsyncQueryEvaluator(
      new BlockingDatabaseWrapper(workPool, database),
      queryFactory
    )
  }

  // operator functions. Declared here so that identity equality works for expectations
  val fromRow  = (r: ResultSet) => r.getInt("1")

  "BlockingEvaluatorWrapper" should {
    "select" in {
      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                               willReturn connection
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") willReturn query
        one(query).select(fromRow)                                         willReturn Seq(1)
        one(database).close(connection)
      }

      newEvaluator().select("SELECT 1")(fromRow).get()
    }

    "selectOne" in {
      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                               willReturn connection
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") willReturn query
        one(query).select(fromRow)                                         willReturn Seq(1)
        one(database).close(connection)
      }

      newEvaluator().selectOne("SELECT 1")(fromRow).get()
    }

    "count" in {
      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                               willReturn connection
        one(queryFactory).apply(connection, QueryClass.Select, "SELECT 1") willReturn query
        one(query).select(any[ResultSet => Int])                           willReturn Seq(1)
        one(database).close(connection)
      }

      newEvaluator().count("SELECT 1").get()
    }

    "execute" in {
      val sql = "INSERT INTO foo (id) VALUES (1)"

      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                         willReturn connection
        one(queryFactory).apply(connection, QueryClass.Execute, sql) willReturn query
        one(query).execute()                                         willReturn 1
        one(database).close(connection)
      }

      newEvaluator().execute("INSERT INTO foo (id) VALUES (1)").get()
    }

    "executeBatch" in {
      val sql = "INSERT INTO foo (id) VALUES (?)"

      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                         willReturn connection
        one(queryFactory).apply(connection, QueryClass.Execute, sql) willReturn query
        one(query).addParams(1)
        one(query).execute()                                         willReturn 1
        one(database).close(connection)
      }

      newEvaluator().executeBatch("INSERT INTO foo (id) VALUES (?)")(_(1)).get()
    }

    "transaction" in {
      val sql = "INSERT INTO foo (id) VALUES (1)"

      expect {
        one(database).openTimeout                                          willReturn 500.millis
        one(database).open()                                         willReturn connection
        one(connection).setAutoCommit(false)
        one(queryFactory).apply(connection, QueryClass.Execute, sql) willReturn query
        one(query).execute()                                         willReturn 1
        one(connection).commit()
        one(connection).setAutoCommit(true)
        one(database).close(connection)
      }

      newEvaluator().transaction(_.execute("INSERT INTO foo (id) VALUES (1)")).get()
    }
  }
}
