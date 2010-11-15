package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import database._


trait ApachePoolingDatabase {
  def sizeMin: Int = 10
  def sizeMax: Int = 10
  def testIdle: Duration = 1.second
  def maxWait: Duration = 10.millis
  def minEvictableIdle: Duration = 0.seconds
  def testOnBorrow: Boolean = false
}

trait TimingOutDatabase {
  def poolSize: Int
  def queueSize: Int
  def open: Duration
  def initialize: Duration
}

trait AutoDisablingDatabase {
  def errorCount: Int
  def interval: Duration
}

trait Database {
  def pool: Option[ApachePoolingDatabase]
  def statsCollector: Option[StatsCollector]
  def autoDisable: Option[AutoDisablingDatabase]
  def timeout: Option[TimingOutDatabase]
  def memoize: Boolean = true

  def apply() = {
    var factory: DatabaseFactory = pool.map(apacheConfig =>
      new ApachePoolingDatabaseFactory(
        apacheConfig.sizeMin,
        apacheConfig.sizeMax,
        apacheConfig.testIdle,
        apacheConfig.maxWait,
        apacheConfig.testOnBorrow,
        apacheConfig.minEvictableIdle)
    ).getOrElse(new SingleConnectionDatabaseFactory)

    statsCollector.foreach { stats =>
      factory = new StatsCollectingDatabaseFactory(factory, stats)
    }

    timeout.foreach { timeoutConfig =>
      factory = new TimingOutDatabaseFactory(factory,
        timeoutConfig.poolSize,
        timeoutConfig.queueSize,
        timeoutConfig.open,
        timeoutConfig.initialize,
        timeoutConfig.poolSize)
    }

    autoDisable.foreach { disable =>
      factory = new AutoDisablingDatabaseFactory(factory, disable.errorCount, disable.interval)
    }

    if (memoize) {
      factory = new MemoizingDatabaseFactory(factory)
    }

    factory
  }
}

trait Connection {
  def hostnames: Seq[String]
  def database: String
  def username: String
  def password: String
  def urlOptions: Map[String, String] = Map()

  def withHost(newHost: String) = {
    val current = this
    new Connection {
      override def database = current.database
      override def username = current.username
      override def password = current.password
      override def urlOptions = current.urlOptions
      override def hostnames = Seq(newHost)
    }
  }

  def withHosts(newHosts: Seq[String]) = {
    val current = this
    new Connection {
      override def database = current.database
      override def username = current.username
      override def password = current.password
      override def urlOptions = current.urlOptions
      override def hostnames = newHosts
    }
  }

  def withDatabase(newDatabase: String) = {
    val current = this
    new Connection {
      override def database = newDatabase
      override def username = current.username
      override def password = current.password
      override def urlOptions = current.urlOptions
      override def hostnames = current.hostnames
    }
  }

  def withoutDatabase = {
    val current = this
    new Connection {
      override def database = null
      override def username = current.username
      override def password = current.password
      override def urlOptions = current.urlOptions
      override def hostnames = current.hostnames
    }
  }
}
