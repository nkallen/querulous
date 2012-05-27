package com.twitter.querulous.config

import com.twitter.querulous._
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import database._


class ApachePoolingDatabase {
  var sizeMin: Int = 10
  var sizeMax: Int = 10
  var testIdle: Duration = 1.second
  var maxWait: Duration = 10.millis
  var minEvictableIdle: Duration = 60.seconds
  var testOnBorrow: Boolean = false
}

class TimingOutDatabase {
  var poolSize: Int = 10
  var queueSize: Int = 10000
  var open: Duration = 1.second
}

trait AutoDisablingDatabase {
  def errorCount: Int
  def interval: Duration
}

class Database {
  var pool: Option[ApachePoolingDatabase] = None
  def pool_=(p: ApachePoolingDatabase) { pool = Some(p) }
  var autoDisable: Option[AutoDisablingDatabase] = None
  def autoDisable_=(a: AutoDisablingDatabase) { autoDisable = Some(a) }
  var timeout: Option[TimingOutDatabase] = None
  def timeout_=(t: TimingOutDatabase) { timeout = Some(t) }
  var memoize: Boolean = true

  def apply(stats: StatsCollector): DatabaseFactory = {
    var factory: DatabaseFactory = pool.map(apacheConfig =>
      new ApachePoolingDatabaseFactory(
        apacheConfig.sizeMin,
        apacheConfig.sizeMax,
        apacheConfig.testIdle,
        apacheConfig.maxWait,
        apacheConfig.testOnBorrow,
        apacheConfig.minEvictableIdle)
    ).getOrElse(new SingleConnectionDatabaseFactory)

    timeout.foreach { timeoutConfig =>
      factory = new TimingOutDatabaseFactory(factory,
        timeoutConfig.poolSize,
        timeoutConfig.queueSize,
        timeoutConfig.open,
        timeoutConfig.poolSize)
    }

    if (stats ne NullStatsCollector) {
      factory = new StatsCollectingDatabaseFactory(factory, stats)
    }

    autoDisable.foreach { disable =>
      factory = new AutoDisablingDatabaseFactory(factory, disable.errorCount, disable.interval)
    }

    if (memoize) {
      factory = new MemoizingDatabaseFactory(factory)
    }

    factory
  }

  def apply(): DatabaseFactory = apply(NullStatsCollector)
}

trait Connection {
  def hostnames: Seq[String]
  def database: String
  def username: String
  def password: String
  var urlOptions: Map[String, String] = Map()

  def withHost(newHost: String) = {
    val current = this
    new Connection {
      def hostnames = Seq(newHost)
      def database = current.database
      def username = current.username
      def password = current.password
      urlOptions = current.urlOptions
    }
  }

  def withHosts(newHosts: Seq[String]) = {
    val current = this
    new Connection {
      def hostnames = newHosts
      def database = current.database
      def username = current.username
      def password = current.password
      urlOptions = current.urlOptions
    }
  }

  def withDatabase(newDatabase: String) = {
    val current = this
    new Connection {
      def hostnames = current.hostnames
      def database = newDatabase
      def username = current.username
      def password = current.password
      urlOptions = current.urlOptions
    }
  }

  def withoutDatabase = {
    val current = this
    new Connection {
      def hostnames = current.hostnames
      def database = null
      def username = current.username
      def password = current.password
      urlOptions = current.urlOptions
    }
  }
}
