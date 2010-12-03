package com.twitter.querulous.query

import java.sql.{ResultSet, Connection}
import scala.collection.mutable
import com.twitter.querulous.StatsCollector
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import config.ConfiggyQuery

trait QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, queryString: String, params: Any*): Query
}

trait Query {
  def select[A](f: ResultSet => A): Seq[A]
  def execute(): Int
  def addParams(params: Any*)
  def cancel()
}

object QueryFactory {
  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]): QueryFactory =
    statsCollector match {
      case Some(s) => new ConfiggyQuery(config)(s)
      case None    => new ConfiggyQuery(config)()
    }
}
