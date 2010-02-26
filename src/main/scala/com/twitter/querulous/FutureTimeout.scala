package com.twitter.querulous

import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration

class FutureTimeout(poolSize: Int, queueSize: Int) {
  private val executor = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](queueSize))

  class Task[T](f: => T)(onTimeout: T => Unit) extends Callable[T] {
    private var cancelled = false
    private var result: Option[T] = None

    def call = {
      try {
        result = Some(f)
        synchronized {
          if (cancelled) {
            throw new TimeoutException
          } else {
            result.get
          }
        }
      } catch {
        case e: TimeoutException =>
          callOnTimeout
          throw e
      }
    }

    def cancel() = synchronized {
      cancelled = true
    }

    def callOnTimeout() = synchronized {
      result.foreach(onTimeout(_))
      result = None
    }
  }

  def apply[T](timeout: Duration)(f: => T)(onTimeout: T => Unit): T = {
    val task = new Task(f)(onTimeout)
    val future = new FutureTask(task)
    try {
      executor.execute(future)
      future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: JTimeoutException =>
        task.cancel
        task.callOnTimeout
        throw new TimeoutException
      case e: RejectedExecutionException =>
        task.cancel
        task.callOnTimeout
        throw new TimeoutException
    }
  }
}
