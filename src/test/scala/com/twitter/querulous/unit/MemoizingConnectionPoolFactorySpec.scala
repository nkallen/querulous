package com.twitter.querulous.unit

import org.specs.mock.JMocker
import org.specs.Specification
import com.twitter.querulous.connectionpool.{ConnectionPool, ConnectionPoolFactory, MemoizingConnectionPoolFactory}

object MemoizingConnectionPoolFactorySpec extends Specification with JMocker {
  val username = "username"
  val password = "password"
  val hosts = List("foo")

  "MemoizingConnectionPoolFactory" should {
    "apply" in {
      val connectionPool1 = mock[ConnectionPool]
      val connectionPool2 = mock[ConnectionPool]
      val connectionPoolFactory = mock[ConnectionPoolFactory]
      val memoizingConnectionPool = new MemoizingConnectionPoolFactory(connectionPoolFactory)

      expect {
        one(connectionPoolFactory).apply(hosts, "bar", username, password) willReturn connectionPool1
        one(connectionPoolFactory).apply(hosts, "baz", username, password) willReturn connectionPool2
      }
      memoizingConnectionPool(hosts, "bar", username, password) mustBe connectionPool1
      memoizingConnectionPool(hosts, "bar", username, password) mustBe connectionPool1
      memoizingConnectionPool(hosts, "baz", username, password) mustBe connectionPool2
      memoizingConnectionPool(hosts, "baz", username, password) mustBe connectionPool2
    }
  }
}
