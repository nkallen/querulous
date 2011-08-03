package com.twitter.querulous.test

import java.sql.ResultSet
import com.twitter.querulous.query.Query

class FakeQuery(resultSets: Seq[ResultSet]) extends Query {
  override def cancel() = {
  }

  override def select[A](f: ResultSet => A) = {
    resultSets.map(f)
  }

  override def execute() = 0

  def addParams(params: Any*) = {}
}
