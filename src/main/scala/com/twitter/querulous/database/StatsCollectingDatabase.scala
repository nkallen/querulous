package com.twitter.querulous.database

class StatsCollectingDatabaseFactory(
  databaseFactory: DatabaseFactory,
  stats: StatsCollector) extends DatabaseFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String): Database = {
    new StatsCollectingDatabase(databaseFactory(dbhosts, dbname, username, password), stats)
  }
}

class StatsCollectingDatabase(pool: Database, stats: StatsCollector)
  extends Database {

  override def reserve(): Connection = {
    stats.time("connection-pool-reserve-timing") {
      pool.reserve()
    }
  }

  override def release(connection: Connection) = {
    stats.time("connection-pool-release-timing") {
      pool.release(connection)
    }
  }

  override def close() = {
    pool.close()
  }
}
