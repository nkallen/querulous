package com.twitter.querulous.async

import java.sql.Connection
import com.twitter.util.Future

import com.twitter.querulous.database.Database


trait AsyncDatabaseFactory {
  def apply(
    hosts: List[String],
    name: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driverName: String
  ): AsyncDatabase

  def apply(hosts: List[String], name: String, username: String, password: String, urlOptions: Map[String, String]): AsyncDatabase = {
    apply(hosts, name, username, password, urlOptions, Database.DEFAULT_DRIVER_NAME)
  }

  def apply(hosts: List[String], name: String, username: String, password: String): AsyncDatabase = {
    apply(hosts, name, username, password, Map.empty, Database.DEFAULT_DRIVER_NAME)
  }

  def apply(hosts: List[String], username: String, password: String): AsyncDatabase = {
    apply(hosts, null, username, password, Map.empty, Database.DEFAULT_DRIVER_NAME)
  }
}

trait AsyncDatabase {
  def withConnection[R](f: Connection => R): Future[R]
}
