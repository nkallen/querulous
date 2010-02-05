package com.twitter.querulous.evaluator

import java.sql.ResultSet
import java.sql.{SQLException, SQLIntegrityConstraintViolationException}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.xrayspecs.{Time, Duration}
import com.twitter.xrayspecs.TimeConversions._

class AutoDisablingQueryEvaluatorFactory(
  queryEvaluatorFactory: QueryEvaluatorFactory,
  disableErrorCount: Int,
  disableDuration: Duration) extends QueryEvaluatorFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String) = {
    new AutoDisablingQueryEvaluator(
      queryEvaluatorFactory(dbhosts, dbname, username, password),
      disableErrorCount,
      disableDuration)
  }
}

class AutoDisablingQueryEvaluator(
  queryEvaluator: QueryEvaluator,
  disableErrorCount: Int,
  disableDuration: Duration) extends QueryEvaluatorProxy(queryEvaluator) {

  private var disabledUntil: Time = Time.never
  private var consecutiveErrors = 0

  override protected def delegate[A](f: => A) = {
    throwIfDisabled()
    try {
      val rv = f
      noteOperationOutcome(true)
      rv
    } catch {
      case e: MySQLIntegrityConstraintViolationException =>
        // user error: don't blame the db.
        throw e
      case e: SQLIntegrityConstraintViolationException =>
        // user error: don't blame the db.
        throw e
      case e: SQLException =>
        noteOperationOutcome(false)
        throw e
      case e: Exception =>
        throw e
    }
  }

  private def noteOperationOutcome(success: Boolean) {
    synchronized {
      if (success) {
        consecutiveErrors = 0
      } else {
        consecutiveErrors += 1
        if (consecutiveErrors >= disableErrorCount) {
          disabledUntil = disableDuration.fromNow
        }
      }
    }
  }

  private def throwIfDisabled() {
    synchronized {
      if (Time.now < disabledUntil) {
        throw new SQLException("Server is temporarily disabled")
      }
    }
  }
}
