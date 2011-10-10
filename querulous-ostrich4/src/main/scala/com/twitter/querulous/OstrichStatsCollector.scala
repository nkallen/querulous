package com.twitter.querulous

import com.twitter.ostrich.stats.StatsProvider
import com.twitter.querulous.StatsCollector

class OstrichStatsCollector(stats: StatsProvider) extends StatsCollector {
  def incr(k: String, c: Int) {
    stats.incr(k, c)
  }

  def time[T](k: String)(f: => T): T = {
    stats.time(k)(f)
  }

  override def addGauge(name: String)(gauge: => Double) {
    stats.addGauge(name)(gauge)
  }
}
