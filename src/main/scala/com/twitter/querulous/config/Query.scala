package com.twitter.querulous.config

import com.twitter.util.Duration


trait QueryTimeout {
  def query: String
  def name: String
  def timeout: Duration
}

trait RetryingQuery {
  def retries: Int
}

trait TimingOutQuery {
  def timeouts: Seq[QueryTimeout]
  def defaultTimeout: Duration
}

trait Query {
  def timeout: Option[TimingOutQuery]
  def retry: Option[RetryingQuery]
  def debug: Boolean = false
}

