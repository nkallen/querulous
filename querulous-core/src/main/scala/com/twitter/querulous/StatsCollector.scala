package com.twitter.querulous

trait StatsCollector {
  def incr(name: String, count: Int)
  def time[A](name: String)(f: => A): A
}

object NullStatsCollector extends StatsCollector {
  def incr(name: String, count: Int) {}
  def time[A](name: String)(f: => A): A = f
}
