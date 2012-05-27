package com.twitter.querulous.query

import java.sql.Connection
import org.apache.commons.dbcp.{DelegatingConnection => DBCPConnection}
import com.mysql.jdbc.{ConnectionImpl => MySQLConnection}
import net.lag.logging.Logger

// Emergency connection destruction toolkit
trait ConnectionDestroying {
  private val log = Logger.get(getClass.getName)

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
      // this should never happen if we use our own ApachePoolingDatabase to get connections.
      error("Could not get access to the delegate connection. Make sure the dbcp connection pool allows access to underlying connections.")
    }

    // "close" the wrapper so that it updates its internal bookkeeping, just do it
    try { conn.close() } catch { case _ => }
  }

  def destroyMysqlConnection(conn: MySQLConnection) {
    log.error("Aborting connection due to failed timeout: %s", conn)
    conn.abortInternal()
  }
}
