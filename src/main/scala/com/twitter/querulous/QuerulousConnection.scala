package com.twitter.querulous

import java.sql.{Statement, Connection, PreparedStatement, CallableStatement, Savepoint}
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}
import java.util.Properties


class QuerulousConnection(wrapped: Connection) extends DelegatingConnection(wrapped) {
  override def close() {
    destroyConnection(wrapped)
  }

  private def destroyConnection(conn: Connection) {
    if (!conn.isClosed)
      conn match {
        case c: DBCPConnection =>
          destroyDbcpWrappedConnection(c)
        case c: MySQLConnection =>
          destroyMysqlConnection(c)
        case _ => error("Unsupported driver type, cannot reliably timeout.")
      }
  }

  private def destroyDbcpWrappedConnection(conn: DBCPConnection) {
    val inner = conn.getInnermostDelegate

    if (inner ne null) {
      destroyConnection(inner)
    } else {
      // might just be a race; move on.
      return
    }

    // "close" the wrapper so that it updates its internal bookkeeping, just do it
    try { conn.close() } catch { case _ => }
  }

  private def destroyMysqlConnection(conn: MySQLConnection) {
    conn.abortInternal()
  }
}

abstract class DelegatingConnection(wrapped: Connection) extends Connection {
  def clearWarnings() { wrapped.clearWarnings() }
  def close() { wrapped.close() }
  def commit() { wrapped.commit() }
  def createArrayOf(typeName: String, elements: Array[Object]) = { wrapped.createArrayOf(typeName, elements) }
  def createBlob() = { wrapped.createBlob() }
  def createClob() = { wrapped.createClob() }
  def createNClob() = { wrapped.createNClob() }
  def createSQLXML() = { wrapped.createSQLXML() }
  def createStatement(): Statement = { wrapped.createStatement() }
  def createStatement(rst: Int, rsc: Int): Statement = { wrapped.createStatement(rst, rsc) }
  def createStatement(rst: Int, rsc: Int, rsh: Int): Statement = { wrapped.createStatement(rst, rsc, rsh) }
  def createStruct(tn: String, att: Array[Object]) = { wrapped.createStruct(tn, att) }
  def getAutoCommit: Boolean = { wrapped.getAutoCommit() }
  def getCatalog() = { wrapped.getCatalog() }
  def getClientInfo(): Properties = { wrapped.getClientInfo() }
  def getClientInfo(name: String): String = { wrapped.getClientInfo(name) }
  def getHoldability() = { wrapped.getHoldability() }
  def getMetaData() = { wrapped.getMetaData() }
  def getTransactionIsolation() = { wrapped.getTransactionIsolation() }
  def getTypeMap() = { wrapped.getTypeMap() }
  def getWarnings() = { wrapped.getWarnings() }
  def isClosed() = { wrapped.isClosed() }
  def isReadOnly() = { wrapped.isReadOnly() }
  def isValid(timeout: Int) = { wrapped.isValid(timeout) }
  def nativeSQL(sql: String) = { wrapped.nativeSQL(sql) }
  def prepareCall(sql: String): CallableStatement = { wrapped.prepareCall(sql) }
  def prepareCall(sql: String, rst: Int, rsc: Int): CallableStatement = { wrapped.prepareCall(sql, rst, rsc) }
  def prepareCall(sql: String, rst: Int, rsc: Int, rsh: Int): CallableStatement = { wrapped.prepareCall(sql, rst, rsc, rsh) }
  def prepareStatement(sql: String): PreparedStatement = { wrapped.prepareStatement(sql) }
  def prepareStatement(sql: String, agk: Int): PreparedStatement = { wrapped.prepareStatement(sql, agk) }
  def prepareStatement(sql: String, ci: Array[Int]): PreparedStatement = { wrapped.prepareStatement(sql, ci) }
  def prepareStatement(sql: String, rst: Int, rsc: Int): PreparedStatement = { wrapped.prepareStatement(sql, rst, rsc) }
  def prepareStatement(sql: String, rst: Int, rsc: Int, rsh: Int): PreparedStatement = { wrapped.prepareStatement(sql, rst, rsc, rsh) }
  def prepareStatement(sql: String, cn: Array[String]): PreparedStatement = { wrapped.prepareStatement(sql, cn) }
  def releaseSavepoint(sp: Savepoint) { wrapped.releaseSavepoint(sp) }
  def rollback() { wrapped.rollback() }
  def rollback(sp: Savepoint) { wrapped.rollback(sp) }
  def setAutoCommit(ac: Boolean) { wrapped.setAutoCommit(ac) }
  def setCatalog(catalog: String) { wrapped.setCatalog(catalog) }
  def setClientInfo(prop: Properties) { wrapped.setClientInfo(prop) }
  def setClientInfo(name: String, value: String) = { wrapped.setClientInfo(name, value) }
  def setHoldability(hold: Int) { wrapped.setHoldability(hold) }
  def setReadOnly(ro: Boolean) { wrapped.setReadOnly(ro) }
  def setSavepoint(): Savepoint = { wrapped.setSavepoint() }
  def setSavepoint(name: String): Savepoint = { wrapped.setSavepoint(name) }
  def setTransactionIsolation(lvl: Int) { wrapped.setTransactionIsolation(lvl) }
  def setTypeMap(map: java.util.Map[String, Class[_]]) { wrapped.setTypeMap(map) }

  def isWrapperFor(iface: Class[_]) = { wrapped.isWrapperFor(iface) }
  def unwrap[T](iface: Class[T]) = { wrapped.unwrap(iface) }
}
