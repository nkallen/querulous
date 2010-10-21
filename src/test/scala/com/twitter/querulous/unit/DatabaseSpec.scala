package com.twitter.querulous.unit

import java.sql.{PreparedStatement, Connection, Types}
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}
import java.util.Properties
import net.lag.configgy.Configgy
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.util.TimeConversions._
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
