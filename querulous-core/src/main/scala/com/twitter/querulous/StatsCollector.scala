package com.twitter.querulous

trait StatsCollector {
  def incr(name: String, count: Int)
  def time[A](name: String)(f: => A): A
  def addGauge(name: String)(gauge: () => Double) {}
}

object NullStatsCollector extends StatsCollector {
  def incr(name: String, count: Int) {}
  def time[A](name: String)(f: => A): A = f
}
