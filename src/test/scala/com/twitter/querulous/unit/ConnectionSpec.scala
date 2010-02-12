package com.twitter.querulous.unit

import java.sql.{Connection => JConnection}
import org.specs.Specification
import org.specs.mock.JMocker

object ConnectionSpec extends Specification with JMocker {
  "Connection" should {
    "delegate" in {
      val sqlConnection = mock[JConnection]
      var connection: Connection = null

      doBefore {
        connection = new Connection(sqlConnection, List("host1", "host2"))
      }

      "commit" >> {
        expect {
          one(sqlConnection).commit()
        }
        connection.commit()
      }

      "close" >> {
        expect {
          one(sqlConnection).close()
        }
        connection.close()
      }

      "rollback" >> {
        expect {
          one(sqlConnection).rollback()
        }
        connection.rollback()
      }

      "prepareStatement" >> {
        expect {
          one(sqlConnection).prepareStatement("SELECT 1 FROM DUAL")
        }
        connection.prepareStatement("SELECT 1 FROM DUAL")
      }

      "setAutoCommit" >> {
        expect {
          one(sqlConnection).setAutoCommit(true)
        }
        connection.setAutoCommit(true)
      }
    }
  }
}

