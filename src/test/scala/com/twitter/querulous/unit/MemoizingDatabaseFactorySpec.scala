package com.twitter.querulous.unit

import org.specs.mock.JMocker
import org.specs.Specification
import com.twitter.querulous.database.{Database, DatabaseFactory, MemoizingDatabaseFactory}


class MemoizingDatabaseFactorySpec extends Specification with JMocker {
  val username = "username"
  val password = "password"
  val hosts = List("foo")

  "MemoizingDatabaseFactory" should {
    "apply" in {
      val database1 = mock[Database]
      val database2 = mock[Database]
      val databaseFactory = mock[DatabaseFactory]
      val memoizingDatabase = new MemoizingDatabaseFactory(databaseFactory)

      expect {
        one(databaseFactory).apply(hosts, "bar", username, password) willReturn database1
        one(databaseFactory).apply(hosts, "baz", username, password) willReturn database2
      }
      memoizingDatabase(hosts, "bar", username, password) mustBe database1
      memoizingDatabase(hosts, "bar", username, password) mustBe database1
      memoizingDatabase(hosts, "baz", username, password) mustBe database2
      memoizingDatabase(hosts, "baz", username, password) mustBe database2
    }

    "not cache" in {
      val database = mock[Database]
      val factory = mock[DatabaseFactory]
      val memoizingDatabase = new MemoizingDatabaseFactory(factory)

      expect {
        exactly(2).of(factory).apply(hosts, username, password) willReturn database
      }

      memoizingDatabase(hosts, username, password) mustBe database
      memoizingDatabase(hosts, username, password) mustBe database
    }
  }
}
