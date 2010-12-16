package com.twitter.querulous

import java.util.concurrent.{ThreadFactory, TimeoutException => JTimeoutException, _}
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.xrayspecs.Duration

class FutureTimeout(poolSize: Int, queueSize: Int) {
  object DaemonThreadFactory extends ThreadFactory {
    val group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "querulous")
    val threadNumber = new AtomicInteger(1)

    def newThread(r: Runnable) = {
      val thread = new Thread(group, r, "querulous-" + threadNumber.getAndIncrement())
      if (!thread.isDaemon) {
        thread.setDaemon(true)
      }
      if (thread.getPriority != Thread.NORM_PRIORITY) {
        thread.setPriority(Thread.NORM_PRIORITY)
      }
      thread
    }
  }
  private val executor = new ThreadPoolExecutor(1, poolSize, 60, TimeUnit.SECONDS,
                                                new LinkedBlockingQueue[Runnable](queueSize),
                                                DaemonThreadFactory)

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
