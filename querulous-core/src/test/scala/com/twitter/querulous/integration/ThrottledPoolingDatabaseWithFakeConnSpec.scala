package com.twitter.querulous.integration

import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.querulous.ConfiguredSpecification
import com.twitter.querulous.sql.{FakeContext, FakeDriver}
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException
import com.twitter.querulous.database.{PoolEmptyException, Database, ThrottledPoolingDatabaseFactory}
import com.twitter.querulous.query.{SqlQueryTimeoutException, TimingOutQueryFactory, SqlQueryFactory}

object ThrottledPoolingDatabaseWithFakeConnSpec {
  // configure repopulation interval to a minute to avoid conn repopulation when test running
  val testDatabaseFactory = new ThrottledPoolingDatabaseFactory(1, 1.second, 1.second, 60.seconds, Map.empty)
  // configure repopulation interval to 1 second so that we can verify the watchdog actually works
  val testRepopulatedDatabaseFactory = new ThrottledPoolingDatabaseFactory(1, 1.second, 1.second, 1.second, Map.empty)
  val testQueryFactory = new TimingOutQueryFactory(new SqlQueryFactory, 500.millis, true)
  val testEvaluatorFactory = new StandardQueryEvaluatorFactory(testDatabaseFactory, testQueryFactory)
  val testRepopulatedEvaluatorFactory = new StandardQueryEvaluatorFactory(testRepopulatedDatabaseFactory, testQueryFactory)
  // configure repopulation interval to 100ms, and connection timeout to 2 seconds
  val testRepopulatedLongConnTimeoutDbFactory = new ThrottledPoolingDatabaseFactory(1, 1.second,
    1.second, 100.milliseconds, Map("connectTimeout" -> "2000"))
  val testRepopulatedLongConnTimeoutEvaluatorFactory = new StandardQueryEvaluatorFactory(
    testRepopulatedLongConnTimeoutDbFactory, testQueryFactory)
}

class ThrottledPoolingDatabaseWithFakeConnSpec extends ConfiguredSpecification {
  import ThrottledPoolingDatabaseWithFakeConnSpec._

  "ThrottledJdbcPoolSpec" should {
    val host = config.hostnames.mkString(",") + "/" + config.database
    config.driverName = FakeDriver.DRIVER_NAME
    val queryEvaluator = testEvaluatorFactory(config)

    FakeContext.setQueryResult(host, "SELECT 1 FROM DUAL", Array(Array[java.lang.Object](1.asInstanceOf[AnyRef])))
    FakeContext.setQueryResult(host, "SELECT 2 FROM DUAL", Array(Array[java.lang.Object](2.asInstanceOf[AnyRef])))
    "execute some queries" >> {
      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      queryEvaluator.select("SELECT 2 FROM DUAL") { r => r.getInt(1) } mustEqual List(2)
    }

    "failfast after a host is down" >> {
      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      FakeContext.markServerDown(host)
      try {
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[CommunicationsException]
        val t0 = Time.now
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[PoolEmptyException]
        (Time.now - t0).inMillis must beCloseTo(0L, 100L)
      } finally {
        FakeContext.markServerUp(host)
      }
    }

    "failfast after connections are closed due to query timeout" >> {
      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      FakeContext.setTimeTakenToExecQuery(host, 1.second)
      try {
        // this will cause the underlying connection being destroyed
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[SqlQueryTimeoutException]
        val t0 = Time.now
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[PoolEmptyException]
        (Time.now - t0).inMillis must beCloseTo(0L, 100L)
      } finally {
        FakeContext.setTimeTakenToExecQuery(host, 0.second)
      }
    }

    "repopulate the pool every repopulation interval" >> {
      val queryEvaluator = testRepopulatedEvaluatorFactory(config)

      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      FakeContext.setTimeTakenToExecQuery(host, 1.second)
      try {
        // this will cause the underlying connection being destroyed
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[SqlQueryTimeoutException]
        Thread.sleep(2000)
        FakeContext.setTimeTakenToExecQuery(host, 0.second)
        // after repopulation, biz as usual
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual  List(1)
      } finally {
        FakeContext.setTimeTakenToExecQuery(host, 0.second)
      }
    }

    "repopulate the pool even if it takes longer to establish a connection than repopulation interval" >> {
      val queryEvaluator = testRepopulatedLongConnTimeoutEvaluatorFactory(config)

      queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual List(1)
      FakeContext.markServerDown(host)
      try {
        // this will cause the underlying connection being destroyed
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } must throwA[CommunicationsException]
        FakeContext.setTimeTakenToOpenConn(host, 1.second)
        FakeContext.markServerUp(host)
        Thread.sleep(2000)
        // after repopulation, biz as usual
        queryEvaluator.select("SELECT 1 FROM DUAL") { r => r.getInt(1) } mustEqual  List(1)
      } finally {
        FakeContext.setTimeTakenToOpenConn(host, 0.second)
        FakeContext.markServerUp(host)
      }
    }
  }
}
