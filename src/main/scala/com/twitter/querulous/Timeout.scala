package com.twitter.querulous

import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.util.Duration
import net.lag.logging.Logger


class TimeoutException extends Exception

object Timeout {
  val defaultTimer = new Timer("Timer thread", true)
  val defaultPurgeInterval = 1000  // TODO(benjy): Set from config?
}

class Timeout(timer: Timer, val purgeInterval: Int) {
  def this(timer: Timer) = this(timer, Timeout.defaultPurgeInterval)
  def this(purgeInterval: Int) = this(Timeout.defaultTimer, purgeInterval)
  def this() = this(Timeout.defaultTimer, Timeout.defaultPurgeInterval)

  private val log = Logger.get(getClass.getName)

  private var numTimeouts = new AtomicInteger(0)

  def apply[T](timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    @volatile var timedOut = false
    val task = if (timeout.inMillis > 0)
      Some(schedule(timer, timeout, { timedOut = true; numTimeouts.incrementAndGet(); onTimeout }))
    else None

    try {
      f
    } finally {
      task map { t =>
        t.cancel()  // Cancel the timeout (regardless of whether it was reached or not).
        // We purge intermittently, so that cancelled tasks can get garbage-collected.
        // We don't want to purge on every cancel, as purge() is linear in the number
        // of tasks scheduled in the timer, and is synchronized on the timer, so if
        // many tasks are cancelled we end up with quadratic behavior.
        if (numTimeouts.get() % purgeInterval == 0)
          timer.purge()
      }
      if (timedOut) throw new TimeoutException
    }
  }

  private def schedule(timer: Timer, timeout: Duration, f: => Unit) = {
    val task = new TimerTask() {
      override def run() {
        try {
          f
        } catch {
          case e: Throwable =>
            log.critical(e, "Timer task tried to throw an exception!")
        }
      }
    }
    timer.schedule(task, timeout.inMillis)
    task
  }
}
