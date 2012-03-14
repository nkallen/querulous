package com.twitter.querulous.query

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp, Types}
import java.lang.reflect.{Field, Modifier}
import java.util.regex.Pattern
import scala.collection.mutable

class SqlQueryFactory extends QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) = {
    new SqlQuery(connection, query, params: _*)
  }
}

class TooFewQueryParametersException(t: Throwable) extends Exception(t)
class TooManyQueryParametersException(t: Throwable) extends Exception(t)

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

class SqlQuery(connection: Connection, val query: String, params: Any*) extends Query {

  def this(connection: Connection, query: String) = {
    this(connection, query, Nil)
  }

  var paramsInitialized = false
  // we need to delay creation of the statement till it's needed
  // so users can add annotations if they want to
  var statementInstance: Option[PreparedStatement] = None
  var additionalParams: Seq[Seq[Any]] = Seq[Seq[Any]]()
  var batchMode = false

  /**
   * Get the statement from the connection, query and params specified.
   */
  def statement = {
    statementInstance.getOrElse {
      val s = buildStatement(connection, query, params: _*)
      // if the user has added more params we need to add those as separate batches
      if (!additionalParams.isEmpty) {
        // add a batch if params have been initialized already
        if (paramsInitialized) s.addBatch()

        // add a batch for each of the additional params
        additionalParams foreach { p =>
          setBindVariable(s, 1, p)
          s.addBatch()
        }
      }

      statementInstance = Some(s)
      s
    }
  }

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
    additionalParams = additionalParams :+ params
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

  /**
   * Convert the annotations into an sql comment.
   */
  private def annotationsAsComment: String = {
    if (annotations.isEmpty) {
      ""
    } else {
      " /*~{" + annotations.map({ case (k,v) => "\"" + quoteString(k) + "\" : \"" +
        quoteString(v) + "\"" }).mkString(", ") + "}*/"
    }
  }

  /**
   * TODO remove: this was lifted from Parser.scala in Scala 2.9, since the 2.8 version didn't escape
   *
   * This function can be used to properly quote Strings
   * for JSON output.
   */
  def quoteString (s : String) : String =
    s.map {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '/'  => "\\/"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      /* We'll unicode escape any control characters. These include:
       * 0x0 -> 0x1f  : ASCII Control (C0 Control Codes)
       * 0x7f         : ASCII DELETE
       * 0x80 -> 0x9f : C1 Control Codes
       *
       * Per RFC4627, section 2.5, we're not technically required to
       * encode the C1 codes, but we do to be safe.
       */
      case c if ((c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f')) => "\\u%04x".format(c: Int)
      case c => c
    }.mkString

  private def buildStatement(connection: Connection, query: String, params: Any*) = {
    val statement = connection.prepareStatement(expandArrayParams(query + annotationsAsComment, params: _*))
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

    def marks(param: Any): String = param match {
      case t2: (_,_) => "(?,?)"
      case t3: (_,_,_) => "(?,?,?)"
      case t4: (_,_,_,_) => "(?,?,?,?)"
      case a: Array[Byte] => "?"
      case s: Iterable[_] => s.toSeq.map(marks(_)).mkString(",")
      case _ => "?"
   }

    while (m.find) {
      try {
        m.appendReplacement(result, marks(params(i)))
      } catch {
        case e: ArrayIndexOutOfBoundsException =>
          throw new TooFewQueryParametersException(e)
        case e: NoSuchElementException =>
          throw new TooFewQueryParametersException(e)
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
        case (a, b) =>
          index = setBindVariable(statement, index, List(a, b)) - 1
        case (a, b, c) =>
          index = setBindVariable(statement, index, List(a, b, c)) - 1
        case (a, b, c, d) =>
          index = setBindVariable(statement, index, List(a, b, c, d)) - 1
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
        case is: Iterable[_] =>
          for (i <- is) index = setBindVariable(statement, index, i)
          index -= 1
        case n: NullValue =>
          statement.setNull(index, n.typeVal)
        case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
          param + " type " + param.asInstanceOf[Object].getClass.getName)
      }
      index + 1
    } catch {
      case e: SQLException =>
        throw new TooManyQueryParametersException(e)
    }
  }
}
