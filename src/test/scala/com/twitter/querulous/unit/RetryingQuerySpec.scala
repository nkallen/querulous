package com.twitter.querulous.unit

import java.sql.{ResultSet, SQLException}
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.querulous.test.FakeQuery
import com.twitter.querulous.query.RetryingQuery


class RetryingQuerySpec extends Specification with JMocker {
  "RetryingQuery" should {
    val retries = 5

    "when the operation throws a SQLException" in {
      var tries = 0

      val query = new FakeQuery(List(mock[ResultSet])) {
        override def select[A](f: ResultSet => A) = {
          tries += 1
          if (tries < retries)
            throw new SQLException
          else
            super.select(f)
        }
      }
      val retryingQuery = new RetryingQuery(query, retries)

      retryingQuery.select { r => 1 } mustEqual List(1)
      tries mustEqual retries
    }

    "when the operation throws a non-SQLException" in {
      var tries = 0

      val query = new FakeQuery(List(mock[ResultSet])) {
        override def select[A](f: ResultSet => A) = {
          tries += 1
          throw new Exception
        }
      }
      val retryingQuery = new RetryingQuery(query, retries)

      retryingQuery.select { r => 1 } must throwA[Exception]
      tries mustEqual 1
    }
  }
}
