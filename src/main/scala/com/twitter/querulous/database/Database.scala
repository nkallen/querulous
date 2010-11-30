package com.twitter.querulous.database

import java.sql.Connection
import com.twitter.querulous.StatsCollector
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


object DatabaseFactory {
  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]) = {
    // this is so lame, why do I have to cast this back?
    val urlOpts = config.getConfigMap("url_options").map(_.asMap.asInstanceOf[Map[String, String]]).getOrElse(Map.empty)

    var factory: DatabaseFactory = if (config.contains("size_min")) {
      new ApachePoolingDatabaseFactory(
        config("size_min").toInt,
        config("size_max").toInt,
        config("test_idle_msec").toLong.millis,
        config("max_wait").toLong.millis,
        config("test_on_borrow").toBoolean,
        config("min_evictable_idle_msec").toLong.millis,
        urlOpts
      )
    } else {
      new SingleConnectionDatabaseFactory(urlOpts)
    }

    statsCollector.foreach { stats =>
      factory = new StatsCollectingDatabaseFactory(factory, stats)
    }

    config.getConfigMap("timeout").foreach { timeoutConfig =>
      factory = new TimingOutDatabaseFactory(factory,
        timeoutConfig("pool_size").toInt,
        timeoutConfig("queue_size").toInt,
        timeoutConfig("open").toLong.millis,
        timeoutConfig("initialize").toLong.millis,
        config("size_max").toInt)
    }

    config.getConfigMap("disable").foreach { disableConfig =>
      factory = new AutoDisablingDatabaseFactory(factory,
                                                 disableConfig("error_count").toInt,
                                                 disableConfig("seconds").toInt.seconds)
    }

    new MemoizingDatabaseFactory(factory)
  }
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
