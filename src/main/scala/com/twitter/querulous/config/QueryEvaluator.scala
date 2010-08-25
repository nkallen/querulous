package com.twitter.querulous.config

import com.twitter.util.Duration


trait AutoDisablingQueryEvaluator {
  val errorCount: Int
  val seconds: Duration
}

trait QueryEvaluator {
  val autoDisable: Option[AutoDisablingQueryEvaluator]
}
