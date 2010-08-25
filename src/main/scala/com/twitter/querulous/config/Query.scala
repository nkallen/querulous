package com.twitter.querulous.config

import com.twitter.util.Duration


trait QueryTimeout {
  val query: String
  val name: String
  val timeout: Duration
}

trait RetryingQuery {
  val retries: Int
}

trait TimingOutQuery {
  val timeouts: Seq[QueryTimeout]
  val defaultTimeout: Duration
}

trait Query {
  val timeout: Option[TimingOutQuery]
  val retry: Option[RetryingQuery]
  val debug: Boolean = false
}

