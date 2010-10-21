package com.twitter.querulous

import java.util.{Timer, TimerTask}
import com.twitter.util.Duration


class TimeoutException extends Exception

object Timeout {
  val defaultTimer = new Timer("Timer thread", true)

  def apply[T](timer: Timer, timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    @volatile var cancelled = false
    val task = if (timeout.inMillis > 0)
      Some(schedule(timer, timeout, { cancelled = true; onTimeout }))
    else None

    try {
      f
    } finally {
      task map { t =>
        t.cancel()
        timer.purge()
      }
      if (cancelled) throw new TimeoutException
    }
  }

  def apply[T](timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    apply(defaultTimer, timeout)(f)(onTimeout)
  }

  private def schedule(timer: Timer, timeout: Duration, f: => Unit) = {
    val task = new TimerTask() {
      override def run() { f }
    }
    timer.schedule(task, timeout.inMillis)
    task
  }
}
