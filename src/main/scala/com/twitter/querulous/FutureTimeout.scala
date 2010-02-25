package com.twitter.querulous

import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration

class FutureTimeout(poolSize: Int, queueSize: Int) {
  private val executor = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](queueSize))

  class Task[T](f: => T)(onTimeout: => Unit) extends Callable[T] {
    @volatile var cancelled = false
    def call = {
      try {
        val result = f
        if (cancelled) {
          throw new TimeoutException
        } else {
          result
        }
      } catch {
        case e: TimeoutException =>
          onTimeout
          throw e
      }
    }
  }

  def apply[T](timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    val task = new Task(f)(onTimeout)
    val future = new FutureTask(task)
    try {
      executor.execute(future)
      future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: JTimeoutException =>
        task.cancelled = true
        throw new TimeoutException
      case e: RejectedExecutionException =>
        task.cancelled = true
        throw new TimeoutException
    }
  }
}




