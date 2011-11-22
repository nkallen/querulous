package com.twitter.querulous.config

import com.twitter.finagle.tracing
import com.twitter.querulous
import querulous.query.TracingQueryFactory

class TracingAsyncQueryEvaluator extends AsyncQueryEvaluator {

  var tracerFactory: tracing.Tracer.Factory = tracing.NullTracer.factory
  var serviceName: String = ""

  protected override def newQueryFactory(stats: querulous.StatsCollector) = {
    new TracingQueryFactory(super.newQueryFactory(stats), serviceName, tracerFactory())
  }

}