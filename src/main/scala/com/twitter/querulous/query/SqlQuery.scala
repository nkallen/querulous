package com.twitter.querulous.query

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp, Types}
import java.lang.reflect.{Field, Modifier}
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
  private val selectTypeValFields = (f: Field) => {
    Modifier.isStatic(f.getModifiers) && classOf[Int].isAssignableFrom(f.getType)
  }

  private val nullTypes = Map(classOf[Types].getFields.filter(selectTypeValFields).map { f: Field =>
    val typeVal = f.getInt(null)
    (typeVal, new NullValue(typeVal) {})
  }: _*)

  val NullString = NullValues(Types.VARCHAR)
  val NullInt = NullValues(Types.INTEGER)
  val NullDouble = NullValues(Types.DOUBLE)
  val NullBoolean = NullValues(Types.BOOLEAN)
  val NullTimestamp = NullValues(Types.TIMESTAMP)
  val NullLong = NullValues(Types.BIGINT)

  /**
   * Gets the NullType for the given SQL type code.
   *
   * @throws NoSuchElementException if {@code typeval} does not correspond to a valid SQL type code
   *     defined in {@link Types}
   */
  def apply(typeVal: Int) = nullTypes(typeVal)
}

class SqlQuery(connection: Connection, query: String, params: Any*) extends Query {

  def this(connection: Connection, query: String) = {
    this(connection, query, Nil)
  }

  var paramsInitialized = false
  val statement = buildStatement(connection, query, params: _*)
  var batchMode = false

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

  def addParams(params: Any*) = {
    if(paramsInitialized && !batchMode) {
      statement.addBatch()
    }
    setBindVariable(statement, 1, params)
    statement.addBatch()
    batchMode = true
  }

  def execute() = {
    withStatement {
      if(batchMode) {
        statement.executeBatch().foldLeft(0)(_+_)
      } else {
        statement.executeUpdate()
      }
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

  private def expandArrayParams(query: String, params: Any*): String = {
    if(params.isEmpty){
      return query
    }
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
        case e: NoSuchElementException => throw new TooFewQueryParametersException
      }
      i += 1
    }
    m.appendTail(result)
    paramsInitialized = true
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
