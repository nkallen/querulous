package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._


trait ApachePoolingDatabase {
  val sizeMin: Int
  val sizeMax: Int
  val testIdle: Duration
  val maxWait: Duration
  val minEvictableIdle: Duration
  val testOnBorrow: Boolean
}

class DefaultApachePoolingDatabase extends ApachePoolingDatabase {
  val sizeMin = 10
  val sizeMax = 10
  val testIdle = 1.second
  val maxWait = 10.millis
  val minEvictableIdle = 0.seconds
  val testOnBorrow = false
}

trait TimingOutDatabase {
  val poolSize: Int
  val queueSize: Int
  val open: Duration
  val initialize: Duration
  val sizeMax: Int
}

trait Database {
  val pool: Option[ApachePoolingDatabase]
  val statsCollector: Option[StatsCollector]
  val timeout: Option[TimingOutDatabase]
  val memoize: Boolean = true
}

trait Connection {
  val hostnames: Seq[String]
  val database: String
  val username: String
  val password: String
}
