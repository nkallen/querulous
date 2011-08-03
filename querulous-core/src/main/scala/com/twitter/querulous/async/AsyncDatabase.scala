package com.twitter.querulous.async

import java.sql.Connection
import com.twitter.util.Future


trait AsyncDatabaseFactory {
  def apply(
    hosts: List[String],
    name: String,
    username: String,
    password: String,
    urlOptions: Map[String, String]
  ): AsyncDatabase

  def apply(hosts: List[String], name: String, username: String, password: String): AsyncDatabase = {
    apply(hosts, name, username, password, Map.empty)
  }

  def apply(hosts: List[String], username: String, password: String): AsyncDatabase = {
    apply(hosts, null, username, password, Map.empty)
  }
}

trait AsyncDatabase {
  def withConnection[R](f: Connection => R): Future[R]
}
