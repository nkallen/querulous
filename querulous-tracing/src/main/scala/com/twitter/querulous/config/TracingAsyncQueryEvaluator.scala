package com.twitter.querulous.config

import com.twitter.finagle.tracing
import com.twitter.querulous
import querulous.query.TracingQueryFactory
import com.twitter.querulous.query.QueryFactory

class TracingAsyncQueryEvaluator extends AsyncQueryEvaluator {

  var tracerFactory: tracing.Tracer.Factory = tracing.NullTracer.factory
  var serviceName: String = ""
  var annotateQuery: Boolean = true // send info such as service name, ip and trace id with query

  protected override def newQueryFactory(stats: querulous.StatsCollector, queryStatsFactory: Option[QueryFactory => QueryFactory]) = {
    new TracingQueryFactory(super.newQueryFactory(stats, queryStatsFactory), serviceName, tracerFactory, annotateQuery)
  }

}