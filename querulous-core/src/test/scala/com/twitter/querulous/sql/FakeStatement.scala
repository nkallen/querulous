package com.twitter.querulous.sql

import java.sql._
import org.apache.commons.dbcp.TesterStatement

class FakeStatement(
  private[this] val conn: Connection,
  private[this] var resultSetType: Int,
  private[this] var resultSetConcurrency: Int
  ) extends TesterStatement(conn, resultSetType, resultSetConcurrency) {

  def this(conn: Connection) = {
    this(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
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
}
