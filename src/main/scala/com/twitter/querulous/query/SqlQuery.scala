package com.twitter.querulous.query

import java.sql.{PreparedStatement, ResultSet, SQLException, Timestamp, Types, Connection}
import java.util.regex.Pattern
import scala.collection.mutable

class SqlQueryFactory extends QueryFactory {
  def apply(connection: Connection, query: String, params: Any*) = {
    new SqlQuery(connection, query, params: _*)
  }
}

class TooFewQueryParametersException extends Exception
class TooManyQueryParametersException extends Exception

sealed abstract case class NullValue(typeVal: Int)
object NullValues {
  case object NullString extends NullValue(Types.VARCHAR)
  case object NullInt extends NullValue(Types.INTEGER)
  case object NullDouble extends NullValue(Types.DOUBLE)
  case object NullBoolean extends NullValue(Types.BOOLEAN)
  case object NullTimestamp extends NullValue(Types.TIMESTAMP)
  case object NullLong extends NullValue(Types.BIGINT)
}

class SqlQuery(connection: Connection, query: String, params: Any*) extends Query {

  val statement = buildStatement(connection, query, params: _*)

  def select[A](f: ResultSet => A): Seq[A] = {
    withStatement {
      statement.executeQuery()
      val rs = statement.getResultSet
      try {
        val finalResult = new mutable.ArrayBuffer[A]
        while (rs.next()) {
          finalResult += f(rs)
        }
        finalResult
      } finally {
        rs.close()
      }
    }
  }

  def execute() = {
    withStatement {
      statement.executeUpdate()
    }
  }

  def cancel() {
    try {
      statement.cancel()
      statement.close()
    } catch {
      case _ =>
    }
  }

  private def withStatement[A](f: => A) = {
    try {
      f
    } finally {
      try {
        statement.close()
      } catch {
        case _ =>
      }
    }
  }

  private def buildStatement(connection: Connection, query: String, params: Any*) = {
    val statement = connection.prepareStatement(expandArrayParams(query, params: _*))
    setBindVariable(statement, 1, params)
    statement
  }

  private def expandArrayParams(query: String, params: Any*) = {
    val p = Pattern.compile("\\?")
    val m = p.matcher(query)
    val result = new StringBuffer
    var i = 0
    while (m.find) {
      try {
        val questionMarks = params(i) match {
          case a: Array[Byte] => "?"
          case s: Seq[_] => s.map { _ => "?" }.mkString(",")
          case _ => "?"
        }
        m.appendReplacement(result, questionMarks)
      } catch {
        case e: ArrayIndexOutOfBoundsException => throw new TooFewQueryParametersException
      }
      i += 1
    }
    m.appendTail(result)
    result.toString
  }

  private def setBindVariable[A](statement: PreparedStatement, startIndex: Int, param: A): Int = {
    var index = startIndex

    try {
      param match {
        case s: String =>
          statement.setString(index, s)
        case l: Long =>
          statement.setLong(index, l)
        case i: Int =>
          statement.setInt(index, i)
        case b: Array[Byte] =>
          statement.setBytes(index, b)
        case b: Boolean =>
          statement.setBoolean(index, b)
        case d: Double =>
          statement.setDouble(index, d)
        case t: Timestamp =>
          statement.setTimestamp(index, t)
        case is: Seq[_] =>
          for (i <- is) index = setBindVariable(statement, index, i)
          index -= 1
        case n: NullValue =>
          statement.setNull(index, n.typeVal)
        case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
          param + " type " + param.asInstanceOf[Object].getClass.getName)
      }
      index + 1
    } catch {
      case e: SQLException => throw new TooManyQueryParametersException
    }
  }
}
