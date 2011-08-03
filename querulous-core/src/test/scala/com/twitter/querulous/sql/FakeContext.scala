package com.twitter.querulous.sql

import scala.collection.mutable
import com.twitter.util.Duration
import com.twitter.conversions.time._
import java.util.concurrent.TimeUnit

object FakeContext {
  private[this] val configMap: mutable.Map[String, FakeServerConfig] =
    new mutable.HashMap[String, FakeServerConfig]();

  def markServerDown(host: String) {
    configMap.synchronized {
       configMap.get(host) match {
         case Some(c) => c.isDown = true
         case None => configMap.put(host, FakeServerConfig(true))
       }
    }
  }

  def markServerUp(host: String) {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.isDown = false
        case None =>
      }
    }
  }

  /**
   * @return true if the server is down; otherwise, false
   */
  def isServerDown(host: String): Boolean = {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.isDown
        case None => false
      }
    }
  }

  def setTimeTakenToOpenConn(host: String, numSecs: Duration) {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.timeTakenToOpenConn = numSecs
        case None => {
          if (numSecs.inSeconds > 0) {
            configMap.put(host, FakeServerConfig(timeTakenToOpenConn = numSecs))
          }
        }
      }
    }
  }

  /**
   * @return time taken to open a connection
   */
  def getTimeTakenToOpenConn(host: String): Duration = {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.timeTakenToOpenConn
        case None => 0.second
      }
    }
  }

  def setTimeTakenToExecQuery(host: String, numSecs: Duration) {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.timeTakenToExecQuery = numSecs
        case None => {
          if (numSecs.inSeconds > 0) {
            configMap.put(host, FakeServerConfig(timeTakenToExecQuery = numSecs))
          }
        }
      }
    }
  }

  /**
   * @return time taken to exec query
   */
  def getTimeTakenToExecQuery(host: String): Duration = {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.timeTakenToExecQuery
        case None => 0.second
      }
    }
  }

  def setQueryResult(host: String, statement: String, result: Array[Array[java.lang.Object]]) {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.resultMap.put(statement, result)
        case None => {
          if (result != null && result.length > 0) {
            configMap.put(host, FakeServerConfig(
              resultMap = mutable.HashMap[String, Array[Array[java.lang.Object]]]((statement, result))))
          }
        }
      }
    }
  }

  def getQueryResult(host: String, statement: String): Array[Array[java.lang.Object]] = {
    configMap.synchronized {
      configMap.get(host) match {
        case Some(c) => c.resultMap.get(statement).getOrElse(Array[Array[java.lang.Object]]())
        case None => Array[Array[java.lang.Object]]()
      }
    }
  }

}

case class FakeServerConfig (
  var isDown: Boolean = false,
  var timeTakenToOpenConn: Duration = Duration(0, TimeUnit.SECONDS),
  var timeTakenToExecQuery: Duration = Duration(0, TimeUnit.SECONDS),
  resultMap: mutable.Map[String, Array[Array[java.lang.Object]]] = mutable.HashMap[String, Array[Array[java.lang.Object]]]())
