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

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    createEvaluatorFactory()(dbhosts, dbname, username, password)
  }

  def apply(dbhosts: List[String], username: String, password: String) = {
    createEvaluatorFactory()(dbhosts, username, password)
  }

  def apply(config: ConfigMap) = {
    createEvaluatorFactory()(config.getList("hostname").toList, config("database"), config("username"), config("password"))
  }
}

trait QueryEvaluatorFactory {
  def apply(dbhost: String, dbname: String, username: String, password: String): QueryEvaluator = {
    apply(List(dbhost), dbname, username, password)
  }
  def apply(dbhost: String, username: String, password: String): QueryEvaluator = apply(List(dbhost), username, password)
  def apply(dbhosts: List[String], dbname: String, username: String, password: String): QueryEvaluator
  def apply(dbhosts: List[String], username: String, password: String): QueryEvaluator
}

trait QueryEvaluator {
  def select[A](query: String, params: Any*)(f: ResultSet => A): Seq[A]
  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Option[A]
  def count(query: String, params: Any*): Int
  def execute(query: String, params: Any*): Int
  def nextId(tableName: String): Long
  def insert(query: String, params: Any*): Long
  def transaction[T](f: Transaction => T): T
}
