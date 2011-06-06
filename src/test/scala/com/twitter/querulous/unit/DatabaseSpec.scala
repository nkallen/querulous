package com.twitter.querulous.unit

import java.sql.{PreparedStatement, Connection, Types}
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}
import java.util.Properties
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.util.TimeConversions._
import com.twitter.querulous.database._
import com.twitter.querulous.ConfiguredSpecification


class DatabaseSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val defaultProps = Map("socketTimeout" -> "41", "connectTimeout" -> "42")

  def mysqlConn(conn: Connection) = conn match {
    case c: DBCPConnection =>
      c.getInnermostDelegate.asInstanceOf[MySQLConnection]
    case c: MySQLConnection => c
  }

  def testFactory(factory: DatabaseFactory) {
    "allow specification of default query options" in {
      val db    = factory(config.hostnames.toList, null, config.username, config.password)
      val props = mysqlConn(db.open).getProperties

      props.getProperty("connectTimeout") mustEqual "42"
      props.getProperty("socketTimeout")  mustEqual "41"
    }

    "allow override of default query options" in {
      val db    = factory(
        config.hostnames.toList,
        null,
        config.username,
        config.password,
        Map("connectTimeout" -> "43"))
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
}
