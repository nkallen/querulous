package com.twitter.querulous

import com.twitter.util.{Time, Future}

trait StatsCollector {
  def incr(name: String, count: Int)
  def time[A](name: String)(f: => A): A
  def addGauge(name: String)(gauge: => Double) {}
  def addMetric(name: String, value: Int) {}

  def timeFutureMillis[T](name: String)(f: Future[T]) = {
    val start = Time.now
    f respond { _ =>
      addMetric(name +"_msec", start.untilNow.inMilliseconds.toInt)
    }
  }
}

object NullStatsCollector extends StatsCollector {
  def incr(name: String, count: Int) {}
  def time[A](name: String)(f: => A): A = f
  override def timeFutureMillis[T](name: String)(f: Future[T]) = f
}
