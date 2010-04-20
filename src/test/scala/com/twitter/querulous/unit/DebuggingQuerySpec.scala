package com.twitter.querulous.unit

import java.sql.{Timestamp, ResultSet}
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.querulous.test.FakeQuery
import com.twitter.querulous.query.DebuggingQuery


class DebuggingQuerySpec extends Specification with JMocker {
  "DebuggingQuery" should {
    val resultSet = mock[ResultSet]
    val logger = mock[String => Unit]

    "log debugging information" in {
      "with no query arguments" >> {
        val query = "SELECT 1 FROM DUAL"
        expect {
          one(logger).apply(query+" ()")
          one(resultSet).getInt("1")
        }

        val debuggingQuery = new DebuggingQuery(new FakeQuery(List(resultSet)), logger, query, List())
        debuggingQuery.select(_.getInt("1"))
      }

      "with query arguments" >> {
        val query = "SELECT 1 FROM DUAL"
        val ts = new Timestamp(1)
        val args = List("asdf", 'c', 1L, 1, Array(0x01: Byte, 0x02: Byte), true, 1.23, ts, List(1, "two", true), "asdf"r)
        val argsAsDebugString = "(\"asdf\", 'c', 1, 1, (2 bytes), true, 1.23, " + ts.toString() + ", (1, \"two\", true), Unknown argument type.)"
        expect {
          one(logger).apply(query+" "+argsAsDebugString)
          one(resultSet).getInt("1")
        }

        val debuggingQuery = new DebuggingQuery(new FakeQuery(List(resultSet)), logger, query, args)
        debuggingQuery.select(_.getInt("1"))
      }
    }
  }
} 
