package com.twitter.querulous

import com.twitter.xrayspecs.{Time, Duration}
import com.twitter.xrayspecs.TimeConversions._
import java.sql.{SQLException, SQLIntegrityConstraintViolationException}


trait AutoDisabler {
  protected val disableErrorCount: Int
  protected val disableDuration: Duration

  private var disabledUntil: Time = Time.never
  private var consecutiveErrors = 0

  protected def throwIfDisabled(throwMessage: String): Unit = {
    synchronized {
      if (Time.now < disabledUntil) {
        throw new SQLException("Server is temporarily disabled: " + throwMessage)
      }
    }
  }

  protected def throwIfDisabled(): Unit = { throwIfDisabled("") }

  protected def noteOperationOutcome(success: Boolean) {
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
}
