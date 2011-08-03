package com.twitter.querulous.unit

import java.sql.{SQLException, Connection}
import scala.collection.mutable
import com.twitter.querulous.TestEvaluator
import com.twitter.querulous.evaluator.StandardQueryEvaluator
import com.twitter.querulous.query._
import com.twitter.querulous.test.FakeDBConnectionWrapper
import com.twitter.querulous.ConfiguredSpecification
import org.specs.mock.{ClassMocker, JMocker}

class QueryEvaluatorSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  import TestEvaluator._

  "QueryEvaluator" should {
    val queryEvaluator = testEvaluatorFactory(config)
    val rootQueryEvaluator = testEvaluatorFactory(config.withoutDatabase)
    val queryFactory = new SqlQueryFactory

    doBefore {
      rootQueryEvaluator.execute("CREATE DATABASE IF NOT EXISTS db_test")
    }

    doAfter {
      queryEvaluator.execute("DROP TABLE IF EXISTS foo")
    }

    "connection pooling" in {
      val connection = mock[Connection]
      val database = new FakeDBConnectionWrapper(connection)

      "transactionally" >> {
        val queryEvaluator = new StandardQueryEvaluator(database, queryFactory)

        expect {
          one(connection).setAutoCommit(false)
          one(connection).prepareStatement("SELECT 1")
          one(connection).commit()
          one(connection).setAutoCommit(true)
        }

        queryEvaluator.transaction { transaction =>
          transaction.selectOne("SELECT 1") { _.getInt("1") }
        }
      }

      "nontransactionally" >> {
        val queryEvaluator = new StandardQueryEvaluator(database, queryFactory)

        expect {
          one(connection).prepareStatement("SELECT 1")
        }

        var list = new mutable.ListBuffer[Int]
        queryEvaluator.selectOne("SELECT 1") { _.getInt("1") }
      }
    }

    "select rows" in {
      var list = new mutable.ListBuffer[Int]
      queryEvaluator.select("SELECT 1 as one") { resultSet =>
        list += resultSet.getInt("one")
      }
      list.toList mustEqual List(1)
    }

    "fallback to a read slave" in {
      // should always succeed if you have the right mysql driver.
      val queryEvaluator = testEvaluatorFactory(
        "localhost:12349" :: config.hostnames.toList, config.database, config.username, config.password)
      queryEvaluator.selectOne("SELECT 1") { row => row.getInt(1) }.toList mustEqual List(1)
      queryEvaluator.execute("CREATE TABLE foo (id INT)") must throwA[SQLException]
    }

    "transaction" in {
      "when there is an exception" >> {
        queryEvaluator.execute("CREATE TABLE foo (bar INT) ENGINE=INNODB")

        try {
          queryEvaluator.transaction { transaction =>
            transaction.execute("INSERT INTO foo VALUES (1)")
            throw new Exception("oh noes")
          }
        } catch {
          case _ =>
        }

        queryEvaluator.select("SELECT * FROM foo")(_.getInt("bar")).toList mustEqual Nil
      }

      "when there is not an exception" >> {
        queryEvaluator.execute("CREATE TABLE foo (bar VARCHAR(50), baz INT) ENGINE=INNODB")

        queryEvaluator.transaction { transaction =>
          transaction.execute("INSERT INTO foo VALUES (?, ?)", "one", 2)
        }

        queryEvaluator.select("SELECT * FROM foo") { row => (row.getString("bar"), row.getInt("baz")) }.toList mustEqual List(("one", 2))
      }
    }

  }
}
