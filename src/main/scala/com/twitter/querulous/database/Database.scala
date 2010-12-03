package com.twitter.querulous.database

import java.sql.Connection
import com.twitter.querulous.StatsCollector
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import config.ConfiggyDatabase

object DatabaseFactory {
  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]) =
    new ConfiggyDatabase(config, statsCollector)()
}

trait DatabaseFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): Database

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database =
    apply(dbhosts, dbname, username, password, Map.empty)

  def apply(dbhosts: List[String], username: String, password: String): Database =
    apply(dbhosts, null, username, password, Map.empty)
}

trait Database {
  def open(): Connection

  def close(connection: Connection)

  def withConnection[A](f: Connection => A): A = {
    val connection = open()
    try {
      f(connection)
    } finally {
      close(connection)
    }
  }

  val defaultUrlOptions = Map(
    "useUnicode" -> "true",
    "characterEncoding" -> "UTF-8",
    "connectTimeout" -> "500"
  )

  protected def url(dbhosts: List[String], dbname: String, urlOptions: Map[String, String]) = {
    val dbnameSegment = if (dbname == null) "" else ("/" + dbname)

    val finalUrlOpts   = defaultUrlOptions ++ urlOptions
    val urlOptsSegment = finalUrlOpts.map(Function.tupled((k, v) => k+"="+v )).mkString("&")

    "jdbc:mysql://" + dbhosts.mkString(",") + dbnameSegment + "?" + urlOptsSegment
  }
}
