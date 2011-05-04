/*
 * Copyright 2009 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.querulous

import java.util.concurrent.CountDownLatch
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time}

/**
 * Generalization of a background process that runs in a thread, and can be
 * stopped. Stopping the thread waits for it to finish running.
 *
 * The code block will be run inside a "forever" loop, so it should either
 * call a method that can be interrupted (like sleep) or block for a low
 * timeout.
 */
abstract class BackgroundProcess(name: String) extends Thread(name) {
  @volatile var running = false
  val startLatch = new CountDownLatch(1)

  override def start() {
    if (!running) {
      running = true
      super.start()
      startLatch.await()
    }
  }

  override def run() {
    startLatch.countDown()
    while (running) {
      try {
        runLoop()
      } catch {
        case e: InterruptedException =>
          running = false
        case e: Throwable =>
          running = false
      }
    }
  }

  def runLoop()

  def shutdown() {
    running = false
    try {
      interrupt()
      join()
    } catch {
      case e: Throwable =>
    }
  }
}

/**
 * Background process that performs some task periodically, over a given duration. If the duration
 * is a useful multiple of seconds, the first event will be staggered so that it takes place on an
 * even multiple. (For example, a minutely event will first trigger at the top of a minute.)
 *
 * The `periodic()` method implements the periodic event.
 */
abstract class PeriodicBackgroundProcess(name: String, private val period: Duration)
extends BackgroundProcess(name) {
  def nextRun: Duration = {
    val t = Time.now + period
    // truncate to nearest round multiple of the desired repeat in seconds.
    if (period >= 1.second) {
      Time.fromSeconds((t.inSeconds / period.inSeconds) * period.inSeconds) - Time.now
    } else {
      t - Time.now
    }
  }

  def runLoop() {
    val delay = nextRun.inMilliseconds
    if (delay > 0) {
      Thread.sleep(delay)
    }

    periodic()
  }

  /**
   * Implement the periodic event here.
   */
  def periodic()
}
