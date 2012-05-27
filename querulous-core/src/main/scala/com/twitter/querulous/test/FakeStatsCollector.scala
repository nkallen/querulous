package com.twitter.querulous.test

import scala.collection.mutable.Map
import com.twitter.util.Time
import com.twitter.querulous.StatsCollector


class FakeStatsCollector extends StatsCollector {
  val counts = Map[String, Int]()
  val times = Map[String, Long]()

  def incr(name: String, count: Int) = {
    counts += (name -> (count+counts.getOrElseUpdate(name, 0)))
  }

  def time[A](name: String)(f: => A): A = {
    val start = Time.now
    val rv = f
    val end = Time.now
    times += (name -> ((end-start).inMillis + times.getOrElseUpdate(name, 0L)))
    rv
  }
}
