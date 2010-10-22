package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._


trait ApachePoolingDatabase {
  def sizeMin: Int
  def sizeMax: Int
  def testIdle: Duration
  def maxWait: Duration
  def minEvictableIdle: Duration
  def testOnBorrow: Boolean
}

class DefaultApachePoolingDatabase extends ApachePoolingDatabase {
  def sizeMin = 10
  def sizeMax = 10
  def testIdle = 1.second
  def maxWait = 10.millis
  def minEvictableIdle = 0.seconds
  def testOnBorrow = false
}

trait TimingOutDatabase {
  def poolSize: Int
  def queueSize: Int
  def open: Duration
  def initialize: Duration
  def sizeMax: Int
}

trait Database {
  def pool: Option[ApachePoolingDatabase]
  def statsCollector: Option[StatsCollector]
  def timeout: Option[TimingOutDatabase]
  def memoize: Boolean = true
}

trait Connection {
  def hostnames: Seq[String]
  def database: String
  def username: String
  def password: String
}
