package com.twitter.querulous.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import query.{QueryClass, QueryFactory}
import database.DatabaseFactory


class ConfiggyDatabase(config: ConfigMap) extends Database {
  val pool = config.getInt("size_min").map { _ =>
    new ApachePoolingDatabase {
      override val sizeMin  = config("size_min").toInt
      override val sizeMax  = config("size_max").toInt
      override val testIdle = config("test_idle_msec").toLong.millis
      override val maxWait  = config("max_wait").toLong.millis
      override val minEvictableIdle = config("min_evictable_idle_msec").toLong.millis
      override val testOnBorrow     = config("test_on_borrow").toBoolean
    }
  }

  override val autoDisable = config.getConfigMap("disable").map { disableConf =>
    new AutoDisablingDatabase {
      val errorCount = disableConf("error_count").toInt
      val interval   = disableConf("seconds").toInt.seconds
    }
  }

  val timeout = config.getConfigMap("timeout").map { timeoutConf =>
    new TimingOutDatabase {
      val poolSize   = timeoutConf("pool_size").toInt
      val queueSize  = timeoutConf("queue_size").toInt
      val open       = timeoutConf("open").toLong.millis
      val initialize = timeoutConf("initialize").toLong.millis
    }
  }
}

class ConfiggyConnection(config: ConfigMap) extends Connection {
  val hostnames = config.getList("hostnames").toList
  val database  = config.getString("database") getOrElse null
  val username  = config("username")
  val password  = config.getString("password") getOrElse null

  override val urlOptions = {
    val opts = config.getConfigMap("url_options")
    opts.map(_.asMap.asInstanceOf[Map[String,String]]) getOrElse Map()
  }
}

class ConfiggyQuery(config: ConfigMap) extends Query {
  private val cancelOnTimeout = config.getList("cancel_on_timeout")

  override val timeouts = config.getConfigMap("timeouts") match {
    case None => {
      config.getLong("query_timeout_default").map(_.millis) match {
        case None => Map[QueryClass,QueryTimeout]()
        case Some(globalTimeout) => {
          Map(QueryClass.classes.values.map { qc =>
            qc -> QueryTimeout(globalTimeout, cancelOnTimeout.contains(qc.name))
          }.toList: _*)
        }
      }
    }
    case Some(timeoutMap) => {
      Map(timeoutMap.keys.map { name =>
        val cancel = cancelOnTimeout.contains(name)
        QueryClass.lookup(name) -> QueryTimeout(timeoutMap(name).toLong.millis, cancel)
      }.toList: _*)
    }
  }

  override val retry = config.getInt("retries").map { retriesInt =>
    new RetryingQuery { val retries = retriesInt }
  }

  override val debug = if (config.getBool("debug", false)) {
    val log = Logger.get(classOf[query.Query].getName)
    Some({ s: String => log.debug(s) })
  } else None
}

class ConfiggyQueryEvaluator(config: ConfigMap) extends QueryEvaluator {
  val database = new ConfiggyDatabase(config.configMap("connection_pool"))
  val query    = new ConfiggyQuery(config)
  val autoDisable = config.getConfigMap("disable").map { disableConf =>
    new AutoDisablingQueryEvaluator {
      val errorCount = disableConf("error_count").toInt
      val interval   = disableConf("seconds").toInt.seconds
    }
  }
}
