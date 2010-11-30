package com.twitter.querulous.unit

import java.sql.{PreparedStatement, Connection, Types}
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}
import java.util.Properties
import net.lag.configgy.{Config, Configgy}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.database._


class DatabaseSpec extends Specification with JMocker with ClassMocker {
  Configgy.configure("config/test.conf")

  val config   = Configgy.config.configMap("db")
  val username = config("username")
  val password = config("password")

  val defaultProps = Map("socketTimeout" -> "41", "connectTimeout" -> "42")

  def mysqlConn(conn: Connection) = conn match {
    case c: DBCPConnection =>
      c.getInnermostDelegate.asInstanceOf[MySQLConnection]
    case c: MySQLConnection => c
  }

  def testFactory(factory: DatabaseFactory) {
    "allow specification of default query options" in {
      val db    = factory(List("localhost"), null, username, password)
      val props = mysqlConn(db.open).getProperties

      props.getProperty("connectTimeout") mustEqual "42"
      props.getProperty("socketTimeout")  mustEqual "41"
    }

    "allow override of default query options" in {
      val db    = factory(List("localhost"), null, username, password, Map("connectTimeout" -> "43"))
      val props = mysqlConn(db.open).getProperties

      props.getProperty("connectTimeout") mustEqual "43"
      props.getProperty("socketTimeout")  mustEqual "41"
    }
  }

  "DatabaseFactory" should {
    "fromConfig" in {
      val poolConfig = Config.fromMap(Map("size_min" -> "0", "size_max" -> "12",
        "test_idle_msec" -> "1000", "test_on_borrow" -> "false", "max_wait" -> "100",
        "min_evictable_idle_msec" -> "5000"))
      val timeoutConfig = Config.fromMap(Map("pool_size" -> "10", "queue_size" -> "7",
        "open" -> "100", "initialize" -> "77"))
      val disableConfig = Config.fromMap(Map("error_count" -> "99", "seconds" -> "34"))
      poolConfig.setConfigMap("timeout", timeoutConfig)
      poolConfig.setConfigMap("disable", disableConfig)

      val factory = DatabaseFactory.fromConfig(poolConfig, None)
      factory must haveClass[MemoizingDatabaseFactory]
      val factory2 = factory.asInstanceOf[MemoizingDatabaseFactory].databaseFactory
      factory2 must haveClass[AutoDisablingDatabaseFactory]
      factory2.asInstanceOf[AutoDisablingDatabaseFactory].disableErrorCount mustEqual 99
      val factory3 = factory2.asInstanceOf[AutoDisablingDatabaseFactory].databaseFactory
      factory3 must haveClass[TimingOutDatabaseFactory]
      factory3.asInstanceOf[TimingOutDatabaseFactory].poolSize mustEqual 10
      val factory4 = factory3.asInstanceOf[TimingOutDatabaseFactory].databaseFactory
      factory4 must haveClass[ApachePoolingDatabaseFactory]
      factory4.asInstanceOf[ApachePoolingDatabaseFactory].minOpenConnections mustEqual 0
      factory4.asInstanceOf[ApachePoolingDatabaseFactory].maxOpenConnections mustEqual 12
    }
  }

  "SingleConnectionDatabaseFactory" should {
    val factory = new SingleConnectionDatabaseFactory(defaultProps)
    testFactory(factory)
  }

  "ApachePoolingDatabaseFactory" should {
    val factory = new ApachePoolingDatabaseFactory(
      10, 10, 1.second, 10.millis, false, 0.seconds, defaultProps
    )

    testFactory(factory)
  }

  "Database#url" should {
    val fake = new Object with Database {
      def open() = null
      def close(connection: Connection) = ()
    }.asInstanceOf[{def url(a: List[String], b:String, c:Map[String, String]): String}]

    "add default unicode urlOptions" in {
      val url = fake.url(List("host"), "db", Map())

      url mustMatch "useUnicode=true"
      url mustMatch "characterEncoding=UTF-8"
    }
  }
}
