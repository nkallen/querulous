package com.twitter.querulous.sql

import java.sql._
import org.apache.commons.dbcp.TesterResultSet

class FakeResultSet(
  private[this] val stmt: Statement,
  private[this] val data: scala.Array[scala.Array[java.lang.Object]],
  private[this] val resultSetType: Int,
  private[this] val resultSetConcurrency: Int
  ) extends TesterResultSet(stmt, data, resultSetType, resultSetConcurrency) {

  private[this] var currentRow: Int = -1

  def this(stmt: Statement) = {
    this(stmt, null, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  }

  def this(stmt: Statement, data: scala.Array[scala.Array[java.lang.Object]]) = {
    this(stmt, data, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  }

  @throws(classOf[SQLException])
  override def next(): Boolean = {
    checkOpen()
    if (this.data != null) {
      currentRow = currentRow + 1;
      currentRow < this.data.length;
    } else {
      false
    }
  }

  @throws(classOf[SQLException])
  override def getString(columnIndex: Int): String = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[String]
  }

  @throws(classOf[SQLException])
  override def getBoolean(columnIndex: Int): Boolean = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Boolean]
  }

  @throws(classOf[SQLException])
  override def getByte(columnIndex: Int): Byte = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Byte]
  }

  @throws(classOf[SQLException])
  override def getShort(columnIndex: Int): Short = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Short]
  }

  @throws(classOf[SQLException])
  override def getInt(columnIndex: Int): Int = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Int]
  }

  @throws(classOf[SQLException])
  override def getLong(columnIndex: Int): Long = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Long]
  }

  @throws(classOf[SQLException])
  override def getFloat(columnIndex: Int): Float = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Float]
  }

  @throws(classOf[SQLException])
  override def getDouble(columnIndex: Int): Double = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[Double]
  }

  /** @deprecated */
  @throws(classOf[SQLException])
  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[java.math.BigDecimal]
  }

  @throws(classOf[SQLException])
  override def getBytes(columnIndex: Int): scala.Array[Byte] = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[scala.Array[Byte]]
  }

  @throws(classOf[SQLException])
  override def getDate(columnIndex: Int): java.sql.Date = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[java.sql.Date]
  }

  @throws(classOf[SQLException])
  override def getTime(columnIndex: Int): java.sql.Time = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[java.sql.Time]
  }

  @throws(classOf[SQLException])
  override def getTimestamp(columnIndex: Int): java.sql.Timestamp = {
    checkOpen()
    data(currentRow)(columnIndex - 1).asInstanceOf[java.sql.Timestamp]
  }

  @throws(classOf[SQLException])
  override def getObject(columnIndex: Int): java.lang.Object = {
    checkOpen()
    data(currentRow)(columnIndex - 1) match {
      case c: AnyRef => c
      case _ => null
    }
  }

  @throws(classOf[SQLException])
  override def isBeforeFirst: Boolean = {
    checkOpen()
    currentRow == -1
  }

  @throws(classOf[SQLException])
  override def isAfterLast: Boolean = {
    checkOpen()
    if(data == null) {
      false
    } else {
      currentRow > data.length
    }
  }

  @throws(classOf[SQLException])
  override def isFirst: Boolean = {
    checkOpen()
    currentRow == 0
  }

  @throws(classOf[SQLException])
  override def isLast: Boolean = {
    checkOpen()
    if (data == null) {
      false
    } else {
      currentRow == data.length - 1
    }
  }

  @throws(classOf[SQLException])
  override def beforeFirst() {
    checkOpen()
    currentRow = -1
  }

  @throws(classOf[SQLException])
  override def afterLast() {
    checkOpen()
    if (data != null) {
      currentRow = data.length
    }
  }

  @throws(classOf[SQLException])
  override def first(): Boolean = {
    checkOpen()
    if (data == null || data.length == 0) {
      false
    } else {
      currentRow = 0
      true
    }
  }

  @throws(classOf[SQLException])
  override def last(): Boolean = {
    checkOpen()
    if (data == null || data.length == 0) {
      false
    } else {
      currentRow = data.length - 1
      true
    }
  }

  @throws(classOf[SQLException])
  override def getRow: Int = {
      checkOpen()
      currentRow + 1
  }

  @throws(classOf[SQLException])
  override protected def checkOpen() {
    this.stmt match {
      case stmt: FakeStatement => stmt.checkOpen()
      case stmt: FakePreparedStatement => stmt.checkOpen()
      case _ => throw new SQLException("Wrong type of statement")
    }

    if(this.isClosed) {
      throw new SQLException("ResultSet is closed.")
    }
  }
}
