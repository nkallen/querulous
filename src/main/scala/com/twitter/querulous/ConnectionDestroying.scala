package com.twitter.querulous.query

import java.sql.Connection
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}

// Emergency connection destruction toolkit
trait ConnectionDestroying {
  def destroyConnection(conn: Connection) {
    if (!conn.isClosed)
      conn match {
        case c: DBCPConnection =>
          destroyDbcpWrappedConnection(c)
        case c: MySQLConnection =>
          destroyMysqlConnection(c)
        case _ => error("Unsupported driver type, cannot reliably timeout.")
      }
  }

  def destroyDbcpWrappedConnection(conn: DBCPConnection) {
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

  def destroyMysqlConnection(conn: MySQLConnection) {
    conn.abortInternal()
  }
}
