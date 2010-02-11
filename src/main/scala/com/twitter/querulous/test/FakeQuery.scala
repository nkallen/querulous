package com.twitter.querulous.fake

import java.sql.ResultSet
import com.twitter.querulous.query.Query

class FakeQuery(resultSets: Seq[ResultSet]) extends Query {
  override def cancel() = {
  }

  override def select[A](f: ResultSet => A) = {
    resultSets.map(f)
  }

  override def execute() = 0
}
