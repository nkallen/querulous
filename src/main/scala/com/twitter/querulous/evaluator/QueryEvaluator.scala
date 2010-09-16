package com.twitter.querulous.evaluator

import java.sql.ResultSet
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import database._
import query._


object QueryEvaluatorFactory {
  def fromConfig(config: ConfigMap, databaseFactory: DatabaseFactory, queryFactory: QueryFactory): QueryEvaluatorFactory = {
    var factory: QueryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
    config.getConfigMap("disable").foreach { disableConfig =>
      factory = new AutoDisablingQueryEvaluatorFactory(factory,
                                                       disableConfig("error_count").toInt,
                                                       disableConfig("seconds").toInt.seconds)
    }
    factory
  }

  def fromConfig(config: ConfigMap, statsCollector: Option[StatsCollector]): QueryEvaluatorFactory = {
    fromConfig(config,
               DatabaseFactory.fromConfig(config.configMap("connection_pool"), statsCollector),
               QueryFactory.fromConfig(config, statsCollector))
  }
}

object QueryEvaluator extends QueryEvaluatorFactory {
  private def createEvaluatorFactory() = {
    val queryFactory = new SqlQueryFactory
    val databaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
    new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    createEvaluatorFactory()(dbhosts, dbname, username, password, urlOptions)
  }
}

trait QueryEvaluatorFactory {
  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator

  def apply(dbhost: String, dbname: String, username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password, urlOptions)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): QueryEvaluator = {
    apply(dbhosts, dbname, username, password, null)
  }

  def apply(dbhost: String, dbname: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password, null)
  }

  def apply(dbhost: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), null, username, password, null)
  }

  def apply(dbhosts: List[String], username: String, password: String): QueryEvaluator = {
    apply(dbhosts, null, username, password, null)
  }

 def apply(config: ConfigMap): QueryEvaluator = {
    apply(
      config.getList("hostname").toList,
      config.getString("database").getOrElse(null),
      config("username"),
      config.getString("password").getOrElse(null),
      // this is so lame, why do I have to cast this back?
      config.getConfigMap("url_options").map(_.asMap.asInstanceOf[Map[String, String]]).getOrElse(null)
    )
  }
}

class ParamsApplier(query: Query) {
  def apply(params: Any*) = query.addParams(params)
}

trait QueryEvaluator {
  def select[A](query: String, params: Any*)(f: ResultSet => A): Seq[A]
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Option[A]
  def count(query: String, params: Any*): Int
  def execute(query: String, params: Any*): Int
  def executeBatch(query: String)(f: ParamsApplier => Unit): Int
  def nextId(tableName: String): Long
  def insert(query: String, params: Any*): Long
  def transaction[T](f: Transaction => T): T
}
