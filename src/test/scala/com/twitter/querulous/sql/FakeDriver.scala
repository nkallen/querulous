package com.twitter.querulous.sql

import java.util.Properties
import java.sql._

class FakeDriver extends Driver {
  @throws(classOf[SQLException])
  def connect(url: String, info: Properties): Connection = {
    if (acceptsURL(url)) {
      val (user, password) = info match {
        case null => ("root", "")
        case _ => (info.getProperty("user", "root"), info.getProperty("password", ""))
      }
      new FakeConnection(url, info, user, password)
    } else {
      null
    }
  }

  @throws(classOf[SQLException])
  def acceptsURL(url: String): Boolean = {
    url.startsWith(FakeDriver.DRIVER_NAME)
  }

  def getMajorVersion: Int = {
    FakeDriver.MAJOR_VERSION
  }

  def getMinorVersion: Int = {
    FakeDriver.MINOR_VERSION
  }

  def jdbcCompliant: Boolean = {
    true
  }

  def getPropertyInfo(url: String, info: Properties): scala.Array[DriverPropertyInfo] = {
    new scala.Array[DriverPropertyInfo](0);
  }

}

object FakeDriver {
  val MAJOR_VERSION: Int = 1
  val MINOR_VERSION: Int = 0
  val DRIVER_NAME: String = "jdbc:twitter:querulous:mockdriver"

  try {
    DriverManager.registerDriver(new FakeDriver());
  } catch {
    case e: SQLException => {
      e.printStackTrace()
    }
  }
}
