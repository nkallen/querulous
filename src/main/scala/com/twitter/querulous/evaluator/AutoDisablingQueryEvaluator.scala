package com.twitter.querulous.evaluator

import java.sql.ResultSet
import java.sql.{SQLException, SQLIntegrityConstraintViolationException}
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException
import com.twitter.querulous.AutoDisabler
import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._


class AutoDisablingQueryEvaluatorFactory(
  queryEvaluatorFactory: QueryEvaluatorFactory,
  disableErrorCount: Int,
  disableDuration: Duration) extends QueryEvaluatorFactory {

  private def chainEvaluator(evaluator: QueryEvaluator) = {
    new AutoDisablingQueryEvaluator(
      evaluator,
      disableErrorCount,
      disableDuration)
  }

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    chainEvaluator(queryEvaluatorFactory(dbhosts, dbname, username, password, urlOptions))
  }
}

class AutoDisablingQueryEvaluator (
  queryEvaluator: QueryEvaluator,
  protected val disableErrorCount: Int,
  protected val disableDuration: Duration) extends QueryEvaluatorProxy(queryEvaluator) with AutoDisabler {

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

}
