package com.twitter.querulous.unit

import java.sql.{PreparedStatement, Connection, Types}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.querulous.query.NullValues._
import com.twitter.querulous.query.{NullValues, SqlQuery}


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

      "arrays of pairs" in {
        val connection = mock[Connection]
        val statement = mock[PreparedStatement]
        expect {
          one(connection).prepareStatement("SELECT * FROM foo WHERE (id, uid) IN ((?,?),(?,?))") willReturn statement
          one(statement).setInt(1, 1) then
          one(statement).setInt(2, 2) then
          one(statement).setInt(3, 3) then
          one(statement).setInt(4, 4) then
          one(statement).executeQuery() then
          one(statement).getResultSet
        }
        new SqlQuery(connection, "SELECT * FROM foo WHERE (id, uid) IN (?)", List((1, 2), (3, 4))).select { _ => 1 }
      }

      "arrays of tuple3s" in {
        val connection = mock[Connection]
        val statement = mock[PreparedStatement]
        expect {
          one(connection).prepareStatement("SELECT * FROM foo WHERE (id1, id2, id3) IN ((?,?,?))") willReturn statement
          one(statement).setInt(1, 1) then
          one(statement).setInt(2, 2) then
          one(statement).setInt(3, 3) then
          one(statement).executeQuery() then
          one(statement).getResultSet
        }
        new SqlQuery(connection, "SELECT * FROM foo WHERE (id1, id2, id3) IN (?)", List((1, 2, 3))).select { _ => 1 }
      }

      "arrays of tuple4s" in {
        val connection = mock[Connection]
        val statement = mock[PreparedStatement]
        expect {
          one(connection).prepareStatement("SELECT * FROM foo WHERE (id1, id2, id3, id4) IN ((?,?,?,?))") willReturn statement
          one(statement).setInt(1, 1) then
          one(statement).setInt(2, 2) then
          one(statement).setInt(3, 3) then
          one(statement).setInt(4, 4) then
          one(statement).executeQuery() then
          one(statement).getResultSet
        }
        new SqlQuery(connection, "SELECT * FROM foo WHERE (id1, id2, id3, id4) IN (?)", List((1, 2, 3, 4))).select { _ => 1 }
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

    "insert nulls" in {
      val queryString = "INSERT INTO TABLE (null1, null2, null3, null4, null5, null6) VALUES (?, ?, ?, ?, ?, ?)"
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]
      expect {
        one(connection).prepareStatement(queryString) willReturn statement then
        one(statement).setNull(1, Types.VARCHAR)
        one(statement).setNull(2, Types.INTEGER)
        one(statement).setNull(3, Types.DOUBLE)
        one(statement).setNull(4, Types.BOOLEAN)
        one(statement).setNull(5, Types.BIGINT)
        one(statement).setNull(6, Types.VARBINARY)
      }

      new SqlQuery(connection, queryString, NullString, NullInt, NullDouble, NullBoolean, NullLong, NullValues(Types.VARBINARY))
    }
  }
}
