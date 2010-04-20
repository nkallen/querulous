package com.twitter.querulous.unit

import java.sql.{PreparedStatement, Connection}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.querulous.query.SqlQuery


class SqlQuerySpec extends Specification with JMocker with ClassMocker {
  "SqlQuery" should {
    "typecast" in {
      "arrays" in {
        val connection = mock[Connection]
        val statement = mock[PreparedStatement]
        expect {
          one(connection).prepareStatement("SELECT * FROM foo WHERE id IN (?,?,?)") willReturn statement
          one(statement).setInt(1, 1) then
          one(statement).setInt(2, 2) then
          one(statement).setInt(3, 3) then
          one(statement).executeQuery() then
          one(statement).getResultSet
        }
        new SqlQuery(connection, "SELECT * FROM foo WHERE id IN (?)", List(1, 2, 3)).select { _ => 1 }
      }
    }

    "create a query string" in {
      val queryString = "INSERT INTO table (col1, col2, col3, col4, col5) VALUES (?, ?, ?, ?, ?)"
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]
      expect {
        one(connection).prepareStatement(queryString) willReturn statement then
        one(statement).setString(1, "one") then
        one(statement).setInt(2, 2) then
        one(statement).setInt(3, 0x03) then
        one(statement).setLong(4, 4) then
        one(statement).setDouble(5, 5.0)
      }

      new SqlQuery(connection, queryString, "one", 2, 0x03, 4L, 5.0)
    }
  }
}
