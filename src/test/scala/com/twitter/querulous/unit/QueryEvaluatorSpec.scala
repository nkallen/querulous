package com.twitter.querulous.unit

import java.sql.{SQLException, DriverManager, Connection}
import scala.collection.mutable
import net.lag.configgy.{Config, Configgy}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.querulous.{StatsCollector, TestEvaluator}
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory, Database}
import com.twitter.querulous.evaluator.{StandardQueryEvaluator, StandardQueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.query._
import com.twitter.querulous.test.FakeDatabase
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class QueryEvaluatorSpec extends Specification with JMocker with ClassMocker {
  Configgy.configure("config/test.conf")
  import TestEvaluator._

  val config = Configgy.config.configMap("db")
  val username = config("username")
  val password = config("password")
  val urlOptions = config.configMap("url_options").asMap.asInstanceOf[Map[String, String]]

  "QueryEvaluator" should {
    val queryEvaluator = testEvaluatorFactory("localhost", "db_test", username, password, urlOptions)
    val rootQueryEvaluator = testEvaluatorFactory("localhost", null, username, password, urlOptions)
    val queryFactory = new SqlQueryFactory

    doBefore {
      rootQueryEvaluator.execute("CREATE DATABASE IF NOT EXISTS db_test")
    }

    doAfter {
      queryEvaluator.execute("DROP TABLE IF EXISTS foo")
    }

    "fromConfig" in {
      val stats = mock[StatsCollector]
      QueryFactory.fromConfig(Config.fromMap(Map.empty), None) must haveClass[SqlQueryFactory]
      QueryFactory.fromConfig(Config.fromMap(Map.empty), Some(stats)) must
        haveClass[StatsCollectingQueryFactory]

      val f1 = QueryFactory.fromConfig(Config.fromMap(Map("query_timeout_default" -> "10")), None)
      f1 must haveClass[PerQueryTimingOutQueryFactory]
      f1.asInstanceOf[PerQueryTimingOutQueryFactory].timeouts mustEqual
        Map(QueryClass.Select -> (10.millis, false), QueryClass.Execute -> (10.millis, false))

      QueryFactory.fromConfig(Config.fromMap(Map("retries" -> "10")), None) must
        haveClass[RetryingQueryFactory]
      QueryFactory.fromConfig(Config.fromMap(Map("debug" -> "true")), None) must
        haveClass[DebuggingQueryFactory]

      val timeoutConfig = new Config()
      timeoutConfig("timeouts.select") = "100"
      timeoutConfig("timeouts.execute") = "5000"
      timeoutConfig("cancel_on_timeout") = List("select")
      val f2 = QueryFactory.fromConfig(timeoutConfig, None)
      f2 must haveClass[PerQueryTimingOutQueryFactory]
      f2.asInstanceOf[PerQueryTimingOutQueryFactory].timeouts mustEqual
        Map(QueryClass.Select -> (100.milliseconds, true), QueryClass.Execute -> (5.seconds, false))
    }

    "connection pooling" in {
      val connection = mock[Connection]
      val database = new FakeDatabase(connection)

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
      val queryEvaluator = testEvaluatorFactory(List("localhost:12349", "localhost"), "db_test", username, password)
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
