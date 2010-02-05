package com.twitter.querulous

import org.specs.runner.SpecsFileRunner
import net.lag.configgy.Configgy

object TestRunner extends SpecsFileRunner("src/test/scala/**/*.scala", ".*",
  System.getProperty("system", ".*"), System.getProperty("example", ".*")) {

  System.setProperty("stage", "test")

  Configgy.configure(System.getProperty("basedir") + "/config/" + System.getProperty("stage", "test") + ".conf")
}
