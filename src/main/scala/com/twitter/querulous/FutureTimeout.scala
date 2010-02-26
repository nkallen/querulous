package com.twitter.querulous

import java.util.concurrent.{TimeoutException => JTimeoutException, _}
import com.twitter.xrayspecs.Duration

class FutureTimeout(poolSize: Int, queueSize: Int) {
  private val executor = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](queueSize))

  class Task[T](f: => T)(onTimeout: T => Unit) extends Callable[T] {
    private var cancelled = false
    private var result: Option[T] = None

    def call = {
      val rv = Some(f)
      synchronized {
        result = rv
        if (cancelled) {
          result.foreach(onTimeout(_))
          throw new TimeoutException
        } else {
          result.get
        }
      }
    }

    def cancel() = synchronized {
      cancelled = true
      result.foreach(onTimeout(_))
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
        task.cancel()
        throw new TimeoutException
      case e: RejectedExecutionException =>
        task.cancel()
        throw new TimeoutException
    }
  }
}
