package com.twitter.querulous

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class DaemonThreadFactory extends ThreadFactory {
  val group        = new ThreadGroup(Thread.currentThread().getThreadGroup(), "querulous")
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
