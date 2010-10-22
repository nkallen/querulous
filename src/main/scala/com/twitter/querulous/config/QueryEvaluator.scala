package com.twitter.querulous.config

import com.twitter.util.Duration


trait AutoDisablingQueryEvaluator {
  def errorCount: Int
  def seconds: Duration
}

trait QueryEvaluator {
  def autoDisable: Option[AutoDisablingQueryEvaluator]
}
