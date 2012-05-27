package com.twitter.querulous.sql

import java.sql._
import org.apache.commons.dbcp.TesterPreparedStatement

class FakePreparedStatement(
  val conn: Connection,
  val sql: String,
  var resultSetType: Int,
  var resultSetConcurrency: Int
  ) extends TesterPreparedStatement(conn, sql, resultSetType, resultSetConcurrency) with PreparedStatement {

  private[this] var resultSet: ResultSet = null

  def this(conn: Connection, sql: String) = {
    this(conn, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  }

  @throws(classOf[SQLException])
  override def executeQuery(sqlQuery: String): ResultSet = {
    checkOpen()
    takeTimeToExecQuery()
    resultSet = new FakeResultSet(this, getFakeResult(sqlQuery), resultSetType, resultSetConcurrency)
    resultSet
  }

  @throws(classOf[SQLException])
  override def executeQuery(): ResultSet = {
    checkOpen()
    takeTimeToExecQuery()
    resultSet = new FakeResultSet(this, getFakeResult(this.sql), resultSetType, resultSetConcurrency)
    resultSet
  }

  @throws(classOf[SQLException])
  override def getResultSet: ResultSet = {
    checkOpen()
    resultSet
  }

  private[this] def getFakeResult(query: String): scala.Array[scala.Array[java.lang.Object]] = {
    FakeContext.getQueryResult(this.conn.asInstanceOf[FakeConnection].host, query)
  }

  @throws(classOf[SQLException])
  override def isClosed: Boolean = {
    !this._open
  }

  @throws(classOf[SQLException])
  override protected[sql] def checkOpen() {
    FakeConnection.checkAliveness(conn.asInstanceOf[FakeConnection])

    if (this.isClosed) {
      throw new SQLException("Statement is closed")
    }

    if(conn.isClosed) {
      throw new SQLException("Connection is closed");
    }
  }

  private def takeTimeToExecQuery() {
    Thread.sleep(FakeContext.getTimeTakenToExecQuery(conn.asInstanceOf[FakeConnection].host).inMillis)
  }
}
